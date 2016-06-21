import random
import struct
import platform
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
import socket
from logger import log
import crypt
from simplequeue import SimpleQueue

IPV6_V4_PREFIX = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'
IPV6_SIZE = 16
GTCP_PACKET_STUB_SIZE = 20
GTCP_CMD_SIZE = 1
GTCP_HEADER_SIZE = GTCP_CMD_SIZE + GTCP_PACKET_STUB_SIZE
GTCP_CMD_RELAY = '\x00'
GTCP_CMD_NONE = '\x01'
GTCP_CMD_ERROR = '\x02'
GTCP_CMD_CONNECT = '\x11'
GTCP_CMD_DISCONNECT = '\x12'
GTCP_CMD_NOTIFY = '\x13'

class TcpEndpoint(object):
	
	def __init__(self, client_id):
		self._client_id = client_id
		self._parsed = False
		
	def __str__(self):
		return self.address
	
	def _parse(self):
		if self._parsed:
			return
		self._raw_ip = self._client_id[:IPV6_SIZE]
		(self._port,) = struct.unpack_from('!H', self._client_id, IPV6_SIZE)
		if self._raw_ip.startswith(IPV6_V4_PREFIX):
			ipv4 = self._raw_ip[len(IPV6_V4_PREFIX):]
			(self._ip,) = struct.unpack('!I', ipv4)
			self._ip_str = socket.inet_ntoa(ipv4)
		else:
			self._ip = self._raw_ip
			self._ip_str = socket.inet_ntop(socket.AF_INET6, self._raw_ip)
		self._address = '%s:%u' % (self._ip_str, self._port)
		self._parsed = True
		
	@property
	def id(self):
		return self._client_id
		
	@property
	def client_id(self):
		return self._client_id
	
	@property
	def address(self):
		self._parse()
		return self._address
	
	@property
	def ip(self):
		self._parse()
		return self._ip
	
	@property
	def port(self):
		self._parse()
		return self._port
	
class WorkerTask(object):
	def __init__(self, client, cmd, packet=''):
		self.client = client
		self.cmd = cmd
		self.packet = packet

class GTcpConnection(object):
	
	HEADER_SIZE = 4
	
	def __init__(self, stream, address, config, on_packet=None, on_close=None):
		self._stream = stream
		self._address = address
		if stream.socket.family == socket.AF_INET6:
			self._remote_ip = socket.inet_pton(socket.AF_INET6, address[0])
		else:
			self._remote_ip = IPV6_V4_PREFIX + socket.inet_aton(address[0])
		self._remote_port = self._address[1]
		self._remote_address = '%s:%d' % address
		self._config = config
		if config.CONNECTION_ID_RANDOM_PADDING:
			padding = random.randint(0, 0xffff)
		else:
			padding = 0
		self._id = struct.pack('!16sHH', self._remote_ip, self._remote_port, padding)
		self._on_packet_callback = on_packet
		self._on_close_callback = on_close
		self._set_keep_alive()
		self._stream.set_close_callback(self._on_close)
		self._recv_header()
		
	@property
	def id(self):
		return self._id
	
	@property
	def remote_address(self):
		return self._remote_address
	
	def closed(self):
		return self._stream.closed()

	def _set_keep_alive(self):
		if not self._config.ENABLE_KEEP_ALIVE:
			return
		stream_socket = self._stream.socket
		stream_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
		if platform.system().lower() == 'linux':
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, self._config.KEEP_ALIVE_OPT['timeout'])
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, self._config.KEEP_ALIVE_OPT['interval'])
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, self._config.KEEP_ALIVE_OPT['count'])
			
	def send(self, data):
		self._stream.write(data)
		
	def send_packet(self, packet):
		header = struct.pack('<I', len(packet))
		self.send(header + packet)
		
	def close(self):
		self._stream.close()
		
	def _recv_header(self):
		self._stream.read_bytes(self.HEADER_SIZE, self._on_recv_header)
		
	def _on_recv_header(self, data):
		if len(data) != self.HEADER_SIZE:
			log.error('tcp_conn_header_size_error|remote=%s,header=%s', self._remote_address, data.encode('hex'))
			self._stream.close()
			return
		(body_size,) = struct.unpack('<I', data)
		if body_size >= self._config.TCP_MAX_PACKET_SIZE:
			log.error('tcp_conn_body_size_overflow|remote=%s,size=%u', self._remote_address, body_size)
			self._stream.close()
			return
		self._stream.read_bytes(body_size, self._on_recv_body)
		
	def _on_recv_body(self, data):
		if self._on_packet_callback is not None:
			self._on_packet_callback(self, data)
		self._stream.read_bytes(self.HEADER_SIZE, self._on_recv_header)
	
	def _on_close(self):
		if self._on_close_callback is not None:
			self._on_close_callback(self)
			
class CallbackTcpServer(TCPServer):
	
	def __init__(self, on_connect=None, *args, **kwargs):
		self._on_connect_callback = on_connect
		super(CallbackTcpServer, self).__init__(*args, **kwargs)
		
	def handle_stream(self, stream, address):
		if self._on_connect_callback:
			self._on_connect_callback(stream, address)

class Processor(object):

	def __init__(self, id, config):
		self._id = id
		self._config = config
		self._client = None
	
	def run(self):
		random.seed()
		self.on_init()
		from gtcpclient import GTcpClient
		log.info('tcp_worker_start|id=%d', self._id)
		self._client = GTcpClient(self._config.WORK_ENDPOINT['address'], self._config.WORK_ENDPOINT['port'], 0)
		while True:
			try:
				request = self._client.receive()
				if request is None:
					log.warn('tcp_worker_lost_connection|client_id=%s,client=%s', self._client.id.encode('hex'), self._client.remote_address)
					self._client.close()
				elif len(request) < GTCP_HEADER_SIZE:
					log.error('tcp_worker_request_packet_error|client_id=%s,client=%s,request=%s', self._client.id.encode('hex'), self._client.remote_address, request.encode('hex'))
					self._client.close()
				else:
					request_cmd = request[:GTCP_CMD_SIZE]
					request_client = TcpEndpoint(request[GTCP_CMD_SIZE:GTCP_HEADER_SIZE])
					reply_body = None
					if request_cmd == GTCP_CMD_RELAY:
						request_body = request[GTCP_HEADER_SIZE:]
						reply_body = self.on_packet(request_client, request_body)
					elif request_cmd == GTCP_CMD_CONNECT:
						reply_body = self.on_client_connect(request_client)
					elif request_cmd == GTCP_CMD_DISCONNECT:
						self.on_client_disconnect(request_client)
					if reply_body is None:
						self._client.send(GTCP_CMD_NONE + request_client.client_id)
					else:
						self._client.send(GTCP_CMD_RELAY + request_client.client_id + reply_body)
			except Exception as ex:
				log.exception('tcp_worker_exception|id=%u,exception=%s', self._id, ex, exc_info=True)
				self._client.close()

	def send_packet(self, client_id, packet):
		return self._client.send(GTCP_CMD_NOTIFY + client_id + packet)
		
	@property
	def id(self):
		return self._id
	
	def on_init(self):
		'''
		to be overridden
		'''
		return
	
	def on_packet(self, client, request):
		'''
		to be overridden
		'''
		return ''

	def on_client_connect(self, client):
		'''
		to be overridden
		'''
		return

	def on_client_disconnect(self, client):
		'''
		to be overridden
		'''
		return

	def on_background(self):
		'''
		to be overridden
		'''
		return
	
class GTcpServer(TCPServer):
	
	def __init__(self, config, processor_class):
		if config.DEBUG:
			from threading import Thread as Process
		else:
			from multiprocessing import Process
		self._config = config
		self._clients = {}
		self._idle_workers = SimpleQueue()
		self._max_queue_size = getattr(config, 'TCP_MAX_QUEUE_SIZE', None)
		self._running_workers = SimpleQueue()
		self._waiting_tasks = SimpleQueue(self._max_queue_size)
		self._worker_processes = []
		
		max_buffer_size = getattr(config, 'TCP_MAX_BUFFER_SIZE', None)
		self._worker_server = CallbackTcpServer(on_connect=self._on_worker_connect, max_buffer_size=max_buffer_size)
		self._worker_server.listen(**config.WORK_ENDPOINT)
		self._connection_server = CallbackTcpServer(on_connect=self._on_client_connect, max_buffer_size=max_buffer_size)
		for listen_port in config.LISTEN_ENDPOINTS:
			self._connection_server.listen(**listen_port)
		for i in xrange(0, config.WORKER_COUNT):
			processor = processor_class(i, config)
			p = Process(target=processor.run)
			p.processor = processor
			self._worker_processes.append(p)
			p.start()

	def _on_client_connect(self, stream, address):
		client = GTcpConnection(stream, address, self._config, self._on_client_packet, self._on_client_close)
		if client.id in self._clients:
			log.error('tcp_server_dup_client|id=%s,remote=%s', client.id.encode('hex'), client.remote_address)
		self._clients[client.id] = client
		self._handle_task(client, GTCP_CMD_CONNECT, '')
		log.info('tcp_server_client_connect|id=%s,remote=%s', client.id.encode('hex'), client.remote_address)
		
	def _on_worker_connect(self, stream, address):
		worker = GTcpConnection(stream, address, self._config, self._on_worker_packet, self._on_worker_close)
		worker.running_task = None
		log.info('tcp_server_worker_connect|id=%s,remote=%s', worker.id.encode('hex'), worker.remote_address)
		self._on_worker_idle(worker)

	def _handle_task(self, client, cmd, data=''):
		task = WorkerTask(client, cmd, data)
		if self._idle_workers.empty():
			self._waiting_tasks.put(task)
		else:
			self._assign_task(self._idle_workers.get(), task)

	def _on_client_packet(self, client, data):
		self._handle_task(client, GTCP_CMD_RELAY, data)
		
	def _on_client_close(self, client):
		log.info('tcp_server_client_close|id=%s,remote=%s', client.id.encode('hex'), client.remote_address)
		if client.id not in self._clients:
			log.error('tcp_server_close_conn_not_found|id=%s,remote=%s', client.id.encode('hex'), client.remote_address)
			return
		self._handle_task(client, GTCP_CMD_DISCONNECT, '')
		del self._clients[client.id]
		
	def _on_worker_packet(self, worker, data):
		client = worker.running_task.client
		packet_size = len(data)
		if packet_size < GTCP_HEADER_SIZE:
			log.error('tcp_worker_reply_error|client_id=%s,client=%s,reply=%s', client.id.encode('hex'), client.remote_address, data.encode('hex'))
			worker.running_task.client.close()
			return
		reply_cmd = data[:1]
		if reply_cmd == GTCP_CMD_RELAY or reply_cmd == GTCP_CMD_NOTIFY:
			reply_client = data[GTCP_CMD_SIZE:GTCP_HEADER_SIZE]
			reply_data = data[GTCP_HEADER_SIZE:]
			if reply_client in self._clients:
				self._clients[reply_client].send_packet(reply_data)
			else:
				log.error('tcp_reply_client_not_found|client_id=%s,reply=%s', reply_client.encode('hex'), reply_data.encode('hex'))
				worker.running_task.client.close()
		if reply_cmd != GTCP_CMD_NOTIFY:
			self._on_worker_idle(worker)
		
	def _on_worker_close(self, worker):
		if worker.running_task is not None:
			worker.running_task.client.close()
		self._idle_workers.remove(worker)
		self._running_workers.remove(worker)
		
	def _on_worker_idle(self, worker):
		if self._waiting_tasks.empty():
			self._idle_workers.put(worker)
		else:
			task = self._waiting_tasks.get()
			self._assign_task(worker, task)
	
	def _assign_task(self, worker, task):
		worker.running_task = task
		worker.send_packet(task.cmd + task.client.id + task.packet)
		self._running_workers.put(worker)
		#TODO: set timeout

def run():
	IOLoop.instance().start()
