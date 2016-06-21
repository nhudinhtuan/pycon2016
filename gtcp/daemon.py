#!/usr/bin/env python

import sys, os, time, atexit
import signal
import platform
import osutils

class Daemon:
	"""
	A generic daemon class.
	
	Usage: subclass the Daemon class and override the run() method
	"""
	def __init__(self, handler, pidfile, output='/dev/null'):
		self.handler = handler
		self.pidfile = pidfile
		self.output = output
	
	def daemonize(self):
		"""
		do the UNIX double-fork magic, see Stevens' "Advanced 
		Programming in the UNIX Environment" for details (ISBN 0201563177)
		http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
		"""
		#judge platform
		if platform.system().lower() == 'windows':
			atexit.register(self.delpid)
			pid = str(os.getpid())
			file(self.pidfile,'w+').write("%s\n" % pid)
			return
		
		#do first fork
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit first parent
				sys.exit(0) 
		except OSError as e:
			sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)
	
		# decouple from parent environment
		#os.chdir("/") 
		os.setsid() 
		os.umask(0)
		
		#ignore signal
		try:
			signal.signal(signal.SIGINT, signal.SIG_IGN)
			signal.signal(signal.SIGHUP, signal.SIG_IGN)
			signal.signal(signal.SIGQUIT, signal.SIG_IGN)
			signal.signal(signal.SIGPIPE, signal.SIG_IGN)
			signal.signal(signal.SIGTTOU, signal.SIG_IGN)
			signal.signal(signal.SIGTTIN, signal.SIG_IGN)
			#signal.signal(signal.SIGCHLD, signal.SIG_IGN)	#Can't ignore this signal, otherwise popen may be abnormal
		except Exception as ex:
			sys.stderr.write("ignore signal fail: %s\n" % ex)
			
		# do second fork
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit from second parent
				sys.exit(0) 
		except OSError as e:
			sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1) 
	
		# redirect standard file descriptors
		output_dir = os.path.dirname(self.output)
		if output_dir and not os.path.exists(output_dir):
			os.makedirs(output_dir)
		sys.stdout.flush()
		sys.stderr.flush()
		si = file('/dev/null', 'r')
		so = file(self.output, 'a+')
		os.dup2(si.fileno(), sys.stdin.fileno())
		os.dup2(so.fileno(), sys.stdout.fileno())
		os.dup2(so.fileno(), sys.stderr.fileno())
	
		# write pidfile
		atexit.register(self.delpid)
		pid = str(os.getpid())
		file(self.pidfile,'w+').write("%s\n" % pid)
	
	def delpid(self):
		os.remove(self.pidfile)

	def start(self):
		"""
		Start the daemon
		"""
		# Check for a pidfile to see if the daemon already runs
		try:
			pf = file(self.pidfile,'r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None
	
		if pid:
			message = "pidfile %s already exist. Daemon already running?\n"
			sys.stderr.write(message % self.pidfile)
			sys.exit(1)
		
		# Start the daemon
		self.daemonize()
		self.run()

	def stop(self):
		"""
		Stop the daemon
		"""
		# Get the pid from the pidfile
		try:
			pf = file(self.pidfile,'r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None
	
		if not pid:
			message = "pidfile %s does not exist. Daemon not running?\n"
			sys.stderr.write(message % self.pidfile)
			return # not an error in a restart

		# Try killing the daemon process	
		try:
			osutils.kill_process_tree(pid)
			if os.path.exists(self.pidfile):
				os.remove(self.pidfile)
		except Exception as err:
			print str(err)
			sys.exit(1)

	def restart(self):
		"""
		Restart the daemon
		"""
		self.stop()
		self.start()

	def run(self):
		"""
		Call handler. It will be called after the process has been
		daemonized by start() or restart().
		"""
		if self.handler is not None:
			self.handler()
		
	def main(self):
		if len(sys.argv) == 2:
			action = sys.argv[1].lower()
			if 'start' == action:
				self.start()
			elif 'stop' == action:
				self.stop()
			elif 'restart' == action:
				self.restart()
			else:
				print "Unknown Command"
				print "Usage: %s start|stop|restart" % sys.argv[0]
				sys.exit(2)
			sys.exit(0)
		else:
			print "Usage: %s start|stop|restart" % sys.argv[0]
			sys.exit(2)
