import time
from gtcp import config
from gtcp.logger import log
from gtcp import tcp_server
from gtcp.buffer_reader import BufferReader
from gtcp.buffer_writer import BufferWriter
from gtcp.pbutils import PBValidator
from gtcp.pbutils import pb_to_str
from gtcp.utils import get_timestamp
from im_server.model import utils
from im_server.model.data import *
from im_server.model.data_schema import *
from im_server.model import cache_manager

class RequestContext(object):

	def __init__(self, processor, conn, header):
		self.processor = processor
		self.conn = conn
		self.header = header

class ParamsError(Exception):
	def __init__(self, value):
		self._value = value
	def __str__(self):
		return self._value

class ProcessorManager(tcp_server.Processor):

	_processors = {}

	def on_client_connect(self, conn):
		log.info('new_connect|client_id=%s,ip=%s', conn.id.encode("hex"), conn.ip)

	def on_client_disconnect(self, conn):
		current_client_id = conn.id.encode("hex")
		username = cache_manager.get_username(current_client_id)
		if username:
			if not cache_manager.remove_username(username):
				log.warn("remove_username_fail|username=%s,client_id=%s", username, current_client_id)
			else:
				log.data("remove_username|username=%s,client_id=%s", username, current_client_id)
		if not cache_manager.remove_client_id(current_client_id):
			log.warn('remove_disconnect_client_fail|client_id=%s', current_client_id)
		else:
			log.data("remove_disconnect_client|client_id=%s", current_client_id)

	def on_packet(self, conn, packet):
		reader = BufferReader(packet, '!')

		header_size = reader.get_uint16()
		header_buff = reader.get_buffer(header_size)
		if reader.error:
			log.error('router_header_error|id=%s,packet=%s', self._id, packet.encode('hex'))
			conn.close()
			return None

		# deserialize header
		try:
			header = PacketHeader.FromString(header_buff)
		except:
			log.error('header_parse_error|id=%s,packet=%s', self._id, packet.encode('hex'))
			conn.close()
			return None
		if header.command not in self._processors:
			log.warn('packet_unknown_cmd|id=%s,version=%s,command=%s', header.id, header.version, header.command)
			return None

		processor, req_pro, reply_pro, request_schema = self._processors[header.command]
		request_data = None
		if reply_pro is None:
			reply_data = None
		else:
			reply_data = reply_pro()

		elapsed = 0
		try:
			request_data_buffer = reader.get_remain()
			if req_pro is not None:
				try:
					request_data = req_pro.FromString(request_data_buffer)
				except Exception as ex:
					raise ParamsError(str(ex))
				if request_schema:
					request_errors = utils.validate_protobuf_data(request_data, request_schema)
					if request_errors:
						raise ParamsError(';'.join(request_errors))
			start_time = time.time()
			context = RequestContext(self, conn, header)
			result = processor(context, request_data, reply_data)
			end_time = time.time()
			elapsed = int((end_time - start_time) * 1000)
		except ParamsError as ex:
			log.error('process_request_data_error|id=%s,version=%s,command=0x%02x,ex=%s,request_data=%s',
				header.id, header.version, header.command, ex, request_data)
			result = Result.ERROR_PARAMS
		except:
			log.exception('process_packet_exception|id=%s,version=%s,command=0x%02x,request_data=%s',
				header.id, header.version, header.command, request_data)
			result = Result.ERROR_SERVER

		log.data('process_request|id=%s,version=%s,command=0x%02x,result=%s,elapsed=%s',
					header.id, header.version, header.command, result, elapsed)

		return self.construct_reply_packet(result, header, reply_data)

	@staticmethod
	def construct_reply_packet(result, header, body=None):
		header.result = result
		header.timestamp = get_timestamp()

		writer = BufferWriter('!')
		header_buff = header.SerializePartialToString()
		writer.add_uint16(len(header_buff))
		writer.add_buffer(header_buff)
		if body is not None:
			body = body.SerializePartialToString()
			writer.add_buffer(body)
		return writer.buffer

	@classmethod
	def register_processor(cls, command, processor, req_pro, reply_pro, request_schema):
		if request_schema:
			request_schema = PBValidator(request_schema)
		if command in cls._processors:
			log.assertion('register_command_duplicated|cmd=%s', command)
		cls._processors[command] = (processor, req_pro, reply_pro, request_schema)
		log.info("register_command|cmd=%s", command)

def register_processor(command, request_type=None, reply_type=None, request_schema=None):
	def _register_processor(func):
		ProcessorManager.register_processor(command, func, request_type, reply_type, request_schema)
		return func
	return _register_processor