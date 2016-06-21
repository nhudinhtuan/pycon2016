import threading
from cmd import Cmd
import os
import sys
curr_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(curr_dir)
sys.path.append(os.path.join(curr_dir, '../'))

from gtcp import event_loop
from gtcp.logger import log
from gtcp.gtcpasyncclient import GTcpAsyncClient
from gtcp.buffer_reader import BufferReader
from gtcp.buffer_writer import BufferWriter
from gtcp.utils import get_timestamp
from im_server.model.data import *
from im_server.model.constant import *

INTRO = """
Welcome to gtcp command line version
"""

SERVER_ADDRESS = "139.59.244.132"
SERVER_PORT = 18800

class DemoClient(Cmd):
	prompt = 'client>'
	intro = INTRO
	id = 0

	def __init__(self):
		Cmd.__init__(self)

		self._tcp_client = GTcpAsyncClient('1', SERVER_ADDRESS, SERVER_PORT, self.on_packet, on_disconnect=self.on_client_network_closed)
		self._thread_stop = threading.Event()
		self._thread = threading.Thread(target=self.connect)
		self._thread.start()
		self._username = None

	def on_packet(self, tcp_client, packet):
		reader = BufferReader(packet, '!')
		header_size = reader.get_uint16()
		header = PacketHeader()
		header.ParseFromString(reader.get_buffer(header_size))
		cmd_name = Command.VALUE_TO_NAME[header.command]
		if header.result != Result.SUCCESS:
			print "server> cmd = %s, error = %s" % (cmd_name, Result.VALUE_TO_NAME[header.result])
			if header.command == Command.CMD_USER_REGISTER:
				self._username = None
			return
		if header.command == Command.CMD_MESSAGE_NOTIFY:
			response_buffer = reader.get_remain()
			message_notify_request = MessageNotifyRequest.FromString(response_buffer)
			print "server> %s send you a message \"%s\"" % (message_notify_request.message.from_id, message_notify_request.message.content)
		else:
			print "server> %s successfully!" % Command.VALUE_TO_NAME[header.command]

	def on_client_network_closed(self):
		self._username = None

	def connect(self):
		event_loop.run()

	def send_request(self, cmd, request_data=None):
		header = PacketHeader()
		header.id = self.id
		header.version = 1
		header.command = cmd
		header.timestamp = get_timestamp()

		writer = BufferWriter('!')
		header_buff = header.SerializePartialToString()
		writer.add_uint16(len(header_buff))
		writer.add_buffer(header_buff)
		if request_data is not None:
			body = request_data.SerializePartialToString()
			writer.add_buffer(body)
		request_buff = writer.buffer
		self._tcp_client.send(request_buff)

	''' HELPER COMMAND LINE '''
	def do_quit(self, args):
		event_loop.IOLoop.current().stop()
		self._tcp_client.close()
		self._thread_stop.set()
		return True

	''' GTCP CLIENT REQUEST '''

	# MESSAGE BROADCAST
	def do_register(self, username):
		if self._username:
			print "error: already register"
			return
		if not username:
			print "usage: register username"
			return
		self.send_request(Command.CMD_USER_REGISTER, UserRegisterRequest(username=username))
		self._username = username

	def do_send_message(self, args):
		if not self._username:
			print "error: please register first"
			return
		try:
			parts = args.split(" ")
			username_list = parts[0].split(",")
			content = parts[1]
		except Exception as err:
			print "usage: send_message target1,target2 message"
			return
		message_info = MessageInfo(from_id=self._username, content=content)
		self.send_request(Command.CMD_MESSAGE_SEND, MessageSendRequest(targets=username_list,message=message_info))

if __name__ == "__main__":
	client = DemoClient()
	client.cmdloop()
