import socket
import struct
import json
import platform
from logger import log

class GTcpClient(object):

	def __init__(self, address, port, timeout=10, retry=True):
		self._address = address
		self._port = port
		self._socket = None
		self._timeout = timeout
		self._retry = retry

	def close(self):
		if self._socket is not None:
			self._socket.close()
			self._socket = None
			
	def send(self, request):
		if self._socket is None:
			if not self._connect():
				return False
		packet = struct.pack('<I%ds' % len(request), len(request), request)
		try:
			self._socket.sendall(packet)
			return True
		except Exception as ex:
			self.close()
			if not self._retry:
				log.exception('tcp_send_fail|error=send_fail,address=%s,port=%u,request=%s', self._address, self._port, request.encode('hex'))
				return False
			log.warn('tcp_send_fail|error=send_fail_will_retry,address=%s,port=%u,retry=0,request=%s,ex=%s', self._address, self._port, request.encode('hex'), ex)
			if not self._connect():
				log.exception('tcp_send_fail|error=retry_reconnect,address=%s,port=%u,retry=0,request=%s', self._address, self._port, request.encode('hex'))
				return False
			try:
				self._socket.sendall(packet)
				return True
			except Exception as ex:
				log.exception('tcp_send_fail|error=retry_send_fail,address=%s,port=%u,retry=1,request=%s', self._address, self._port, request.encode('hex'))
				self.close()
				return False

	def receive(self):
		if self._socket is None:
			if not self._connect():
				return None
		try:
			length_data = self._recv(4)
			length = struct.unpack('<I', length_data)[0]
			return self._recv(length)
		except Exception as ex:
			log.warn('tcp_recv_fail|address=%s,port=%u,ex=%s', self._address, self._port, ex, exc_info=True)
			self.close()
			return None

	def request(self, request):
		if self._socket is None:
			if not self._connect():
				return None
		packet = struct.pack('<I%ds' % len(request), len(request), request)
		try:
			self._socket.sendall(packet)
			length_data = self._recv(4)
		except Exception as ex:
			self.close()
			if not self._retry:
				log.exception('tcp_request_fail|error=recv_length_fail,address=%s,port=%u,request=%s', self._address, self._port, request.encode('hex'))
				return None
			if isinstance(ex, socket.timeout):
				log.exception('tcp_request_fail|error=recv_length_timeout,address=%s,port=%u,retry=0,request=%s', self._address, self._port, request.encode('hex'))
				return None
			log.warn('tcp_request_fail|error=recv_length_fail_will_retry,address=%s,port=%u,retry=0,request=%s,ex=%s', self._address, self._port, request.encode('hex'), ex)
			if not self._connect():
				log.exception('tcp_request_fail|error=retry_reconnect,address=%s,port=%u,retry=0,request=%s', self._address, self._port, request.encode('hex'))
				return None
			try:
				self._socket.sendall(packet)
				length_data = self._recv(4)
			except Exception as ex:
				log.exception('tcp_request_fail|error=retry_recv_length_fail,address=%s,port=%u,retry=1,request=%s', self._address, self._port, request.encode('hex'))
				self.close()
				return None
		try:
			length = struct.unpack('<I', length_data)[0]
			return self._recv(length)
		except Exception as ex:
			log.exception('tcp_request_fail|error=recv_data_fail,address=%s,port=%u,request=%s', self._address, self._port, request.encode('hex'))
			self.close()
			return None

	def request_json(self, request):
		request_data = json.dumps(request)
		reply = self.request(request_data)
		if reply is None:
			return None
		try:
			return json.loads(reply)
		except Exception as ex:
			log.warn('tcp_request_parse_json_fail|address=%s,port=%u,reply=%s,ex=%s', self._address, self._port, reply, ex)
			self.close()
			return None

	def _connect(self):
		try:
			self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			if self._timeout > 0:
				self._socket.settimeout(self._timeout)
			self._set_keep_alive()
			self._socket.connect((self._address, self._port))
			log.info('tcp_connect|address=%s,port=%u', self._address, self._port)
			return True
		except Exception as ex:
			log.exception('tcp_connect_fail|address=%s,port=%u,ex=%s', self._address, self._port, ex)
			self.close()
			return False

	def _recv(self, length):
		data = ''
		while length > 0:
			recv_data = self._socket.recv(length)
			recv_length = len(recv_data)
			if recv_length <= 0:
				raise Exception('socket_closed')
			data += recv_data
			length -= recv_length
		return data

	def _set_keep_alive(self):
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
		if platform.system().lower() == 'linux':
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 30)
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)
