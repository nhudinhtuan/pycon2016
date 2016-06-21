from tornado.tcpclient import TCPClient
import socket
import platform
import struct
from logger import log
import event_loop

class GTcpAsyncClient(object):

	HEADER_SIZE = 4
	TCP_MAX_PACKET_SIZE = 256 * 1024

	def __init__(self, id, address, port, on_receive_packet, on_connect=None, on_disconnect=None):
		self._id = id
		self._address = address
		self._port = port
		self._on_receive_packet_callback = on_receive_packet
		self._on_connect_callback = on_connect
		self._on_disconnect_callback = on_disconnect
		self._stream = None
		self._pending_buffer = []
		self._async_connect()

	@property
	def id(self):
		return self._id

	def send(self, packet):
		packet_length = struct.pack('<I', len(packet))
		if self._stream is not None:
			self._stream.write(packet_length + packet)
		else:
			self._pending_buffer.append(packet_length)
			self._pending_buffer.append(packet)

	def close(self):
		event_loop.add_callback(self._close)

	def clear_buffer(self):
		self._pending_buffer = []

	def _async_connect(self):
		log.info('tcp_asnyc_client_try_connect|id=%s,address=%s,port=%s', self._id, self._address, self._port)
		TCPClient().connect(self._address, self._port).add_done_callback(self._on_connect)

	def _on_connect(self, future):
		try:
			self._stream = future.result()
		except:
			log.exception('tcp_asnyc_client_connect_fail|id=%s,address=%s,port=%s', self._id, self._address, self._port)
			self._close()
			self._async_connect()
			return
		log.info('tcp_asnyc_client_connect|id=%s,address=%s,port=%s', self._id, self._address, self._port)
		self._stream.set_close_callback(self._on_close)
		self._set_keep_alive()
		if self._on_connect_callback is not None:
			try:
				self._on_connect_callback(self)
			except:
				log.exception('tcp_asnyc_client_on_connect_exception|id=%s,address=%s,port=%s', self._id, self._address, self._port)
		if self._pending_buffer:
			self._stream.write(''.join(self._pending_buffer))
			self._pending_buffer = []
		self._recv_header()

	def _close(self):
		if self._stream:
			self._stream.close()
			self._stream = None

	def _on_close(self):
		log.info('tcp_asnyc_client_disconnect|id=%s,address=%s,port=%s', self._id, self._address, self._port)
		if self._on_disconnect_callback is not None:
			try:
				self._on_disconnect_callback(self)
			except:
				log.exception('tcp_asnyc_client_on_disconnect_exception|id=%s,address=%s,port=%s', self._id, self._address, self._port)
		self._async_connect()

	def _recv_header(self):
		self._stream.read_bytes(self.HEADER_SIZE, self._on_recv_header)

	def _on_recv_header(self, data):
		(body_size,) = struct.unpack('<I', data)
		if body_size > self.TCP_MAX_PACKET_SIZE:
			log.error('tcp_asnyc_client_body_size_overflow|id=%s,size=%u', self._id, body_size)
			self._close()
			return
		self._stream.read_bytes(body_size, self._on_recv_body)

	def _on_recv_body(self, data):
		if self._on_receive_packet_callback is not None:
			try:
				self._on_receive_packet_callback(self, data)
			except:
				log.exception('tcp_asnyc_client_on_receive_exception|id=%s,packet=%s', self._id, data.encode('hex'))
		self._recv_header()

	def _set_keep_alive(self):
		stream_socket = self._stream.socket
		stream_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
		if platform.system().lower() == 'linux':
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 30)
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)
			stream_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)
