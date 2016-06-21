if __name__ == "__main__":
	import os
	import sys
	curr_dir = os.path.dirname(os.path.abspath(__file__))
	os.chdir(curr_dir)
	sys.path.append(os.path.join(curr_dir, '../'))

	from im_server import config
	from im_server.processors import *
	from im_server.model.data import *

	def main():
		from gtcp import tcp_server
		server = tcp_server.GTcpServer(config, manager.ProcessorManager)
		tcp_server.run()

	if not config.DEBUG:
		from gtcp.daemon import Daemon
		Daemon(main, os.path.abspath(__file__).replace('.py', '.pid'), './log/daemon.log').main()
	else:
		main()
