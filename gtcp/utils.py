import inspect
import os
import sys
import copy
import time
import json
from contextlib import contextmanager
from functools import wraps
from logger import log
from convert import *
import crypt


class ClassPropertyMetaClass(type):
	def __setattr__(self, key, value):
		if key in self.__dict__:
			obj = self.__dict__.get(key)
			if obj and type(obj) is ClassPropertyDescriptor:
				return obj.__set__(self, value)
		super(ClassPropertyMetaClass, self).__setattr__(key, value)

class ClassPropertyDescriptor(object):
	def __init__(self, fget, fset=None):
		self.fget = fget
		self.fset = fset

	def __get__(self, obj, klass=None):
		if klass is None:
			klass = type(obj)
		return self.fget.__get__(obj, klass)()

	def __set__(self, obj, value):
		if not self.fset:
			raise AttributeError("can't set attribute")
		if inspect.isclass(obj):
			type_ = obj
			obj = None
		else:
			type_ = type(obj)
		return self.fset.__get__(obj, type_)(value)

	def setter(self, func):
		if not isinstance(func, (classmethod, staticmethod)):
			func = classmethod(func)
		self.fset = func
		return self

def classproperty(func):
	"""
		-- usage --
		1) simple read:
				...
				@classproperty
				def MY_VALUE(cls):
					return cls._MY_VALUE
				...

		2) simple write: (write by instance)
				...
				// add setter
				@MY_VALUE.setter
				def MY_VALUE(cls, value):
					cls._MY_VALUE = value

		3) complete write: (write by instance or class)
				...
				// add metaclass
				__metaclass__ = ClassPropertyMetaClass

	"""
	if not isinstance(func, (classmethod, staticmethod)):
		func = classmethod(func)
	return ClassPropertyDescriptor(func)

class Object:
	def __init__(self, **kwargs): 
		self.__dict__.update(kwargs)
		
def dict_to_object(d):
	return Object(**d)

def create_object(**kwargs):
	return Object(**kwargs)

def find_first(f, seq):
	"""Return first item in sequence where f(item) == True."""
	for item in seq:
		if f(item): 
			return item
	return None

def get_timestamp():
	return int(time.time())

def find_str(text, prefix, suffix=None):
	start = text.find(prefix)
	if start < 0:
		return None
	start += len(prefix)
	if suffix is None:
		return text[start:]
	end = text.find(suffix, start)
	if end < 0:
		return None
	return text[start:end]

def truncate_unicode(text, max_length, encoding='utf-8', ending=u'...'):
	encoded_str = text.encode(encoding)
	if len(encoded_str) <= max_length:
		return text
	max_length -= len(ending)
	if max_length < 0:
		max_length = 0
	encoded_str = encoded_str[:max_length]
	return encoded_str.decode(encoding, 'ignore') + ending

def exception_safe(exception_return=None, keyword=None, return_filter=copy.copy):
	def _exception_safe(func):
		@wraps(func)
		def _func(*args, **kwargs):
			try:
				return func(*args, **kwargs)
			except:
				if keyword is None:
					log.exception('%s_exception', func.__name__)
				else:
					log.exception('%s_exception', keyword)
				if return_filter:
					return return_filter(exception_return)
				else:
					return exception_return
		return _func
	return _exception_safe

@contextmanager
def directory(path):
	current_dir = os.getcwd()
	os.chdir(path)
	try:
		yield
	finally:
		os.chdir(current_dir)

IS_DJANGO_APP = False
IS_FLASK_APP = False

try:
	from django.conf import settings
	if not settings.configured:
		raise Exception()
	from django_utils import *
	IS_DJANGO_APP = True
except:
	try:
		from flask_utils import *
		IS_FLASK_APP = True
	except:
		pass

def with_cache(cache_key_prefix, timeout=60*60):
	def _with_cache(func):
		@wraps(func)
		def _func(key):
			cache_key = cache_key_prefix + crypt.md5(key)
			cache_data = cache.get(cache_key)
			if cache_data is not None:
				if cache_data[0] == key:
					return cache_data[1]
			data = func(key)
			cache_data = (key, data)
			cache.set(cache_key, cache_data, timeout)
			return data
		return _func
	return _with_cache
