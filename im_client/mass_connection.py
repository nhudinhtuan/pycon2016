import os
import sys
import thread
import time
import sys

# watch -n 1 "netstat -ant | grep 18800 | grep ESTABLISHED -c"

curr_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(curr_dir)
sys.path.append(os.path.join(curr_dir, '../'))

from gtcp.gtcpclient import GTcpClient

class MassConnection:
	SERVER_ADDRESS = "139.59.244.132"
	SERVER_PORT = 18800

	def __init__(self):
		pass

	def test_number_of_connections(self, no_conn):
		print("*** Testing number of connections ***")
		tcp_clients = []
		count = 1
		client = GTcpClient(self.SERVER_ADDRESS, self.SERVER_PORT)

		while client._connect():
			tcp_clients.append(client)
			client = GTcpClient(self.SERVER_ADDRESS, self.SERVER_PORT)
			if count > no_conn:
				break
			if count % 1000 == 0:
				print "Finish initializing %d connections" % (len(tcp_clients))
			count += 1
		print ("Sleep for 1000 seconds")
		time.sleep(1000)
		for client in tcp_clients:
			client.close()

if __name__ == "__main__":
	number_connection = int(input('Enter the number of connection: '))
	massConn = MassConnection()
	massConn.test_number_of_connections(number_connection)