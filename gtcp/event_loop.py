import tornado
from tornado.ioloop import IOLoop
from functools import partial

def create():
	IOLoop().make_current()

def run():
	IOLoop.current().start()

def add_callback(func, *args, **kwargs):
	IOLoop.current().add_callback(func, *args, **kwargs)

def add_timeout(seconds, func, *args, **kwargs):
	return IOLoop.current().call_later(seconds, func, *args, **kwargs)

def remove_timeout(timeout):
	IOLoop.current().remove_timeout(timeout)

def add_interval_timer(seconds, func, *args, **kwargs):
	timer = tornado.ioloop.PeriodicCallback(partial(func, *args, **kwargs), seconds * 1000, IOLoop.instance())
	timer.start()
	return timer
