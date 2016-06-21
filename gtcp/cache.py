import sys
import re
try:
	import cPickle as pickle
except ImportError:  # pragma: no cover
	import pickle
import random
from collections import defaultdict
import platform
import re
from conhash import ConHash

_PY2 = sys.version_info[0] == 2

if _PY2:

	_integer_types = (int, long)

	_iteritems = lambda d: d.iteritems()
	
	def _to_native(x, charset='utf-8', errors='strict'):
		if x is None or isinstance(x, str):
			return x
		return x.encode(charset, errors)

else:

	_integer_types = (int, )
	
	_iteritems = lambda d: iter(d.items())
	
	def _to_native(x, charset='utf-8', errors='strict'):
		if x is None or isinstance(x, str):
			return x
		return x.decode(charset, errors)

def _items(dictorseq):
	"""Wrapper for efficient iteration over dict represented by dicts
	or sequences::

		>>> for k, v in _items((i, i*i) for i in xrange(5)):
		...	assert k*k == v

		>>> for k, v in _items(dict((i, i*i) for i in xrange(5))):
		...	assert k*k == v

	"""
	if hasattr(dictorseq, 'items'):
		return _iteritems(dictorseq)
	return dictorseq

_DEFAULT_CACHE_TIMEOUT = 300
_DEFAULT_SOCKET_TIMEOUT = 3

if platform.system().lower() == 'linux':
	import socket
	_TCP_KEEP_ALIVE_OPTIONS = {
		socket.TCP_KEEPIDLE: 30,
		socket.TCP_KEEPINTVL: 5,
		socket.TCP_KEEPCNT: 5,
	}
else:
	_TCP_KEEP_ALIVE_OPTIONS = {}

class BaseCache(object):
	"""Baseclass for the cache systems.  All the cache systems implement this
	API or a superset of it.

	:param default_timeout: the default timeout (in seconds) that is used if no
							timeout is specified on :meth:`set`.
	"""

	def __init__(self, config):
		self._client = None
		self.default_timeout = config.get('default_timeout', _DEFAULT_CACHE_TIMEOUT)

	@property
	def raw_client(self):
		"""Get raw cache client.

		:returns: Underlying cache client object.
		"""
		return self._client

	def get(self, key):
		"""Look up key in the cache and return the value for it.

		:param key: the key to be looked up.
		:returns: The value if it exists and is readable, else ``None``.
		"""
		return None

	def delete(self, key, noreply=False):
		"""Delete `key` from the cache.

		:param key: the key to delete.
		:param noreply: instructs the server to not send the reply.
		:returns: Whether the key has been deleted.
		:rtype: boolean
		"""
		return True

	def get_list(self, keys):
		"""Returns a list of values for the given keys.
		For each key a item in the list is created::

			foo, bar = cache.get_list("foo", "bar")

		Has the same error handling as :meth:`get`.

		:param keys: The function accepts multiple keys as positional
					 arguments.
		"""
		values = self.get_many(keys)
		return [values.get(key) for key in keys]

	def get_many(self, keys):
		"""Like :meth:`get_list` but return a dict::
		If the given key is missing, it will be missing from the response dict.

			d = cache.get_many("foo", "bar")
			foo = d["foo"]
			bar = d["bar"]

		:param keys: The function accepts multiple keys as positional
					 arguments.
		"""
		return dict([(key, self.get(key)) for key in keys])

	def set(self, key, value, timeout=None, noreply=False):
		"""Add a new key/value to the cache (overwrites value, if key already
		exists in the cache).

		:param key: the key to set
		:param value: the value for the key
		:param timeout: the cache timeout for the key.
						If not specified, it uses the default timeout.
						If specified 0, it will never expire.
		:param noreply: instructs the server to not send the reply.
		:returns: Whether the key existed and has been set.
		:rtype: boolean
		"""
		return True

	def add(self, key, value, timeout=None, noreply=False):
		"""Works like :meth:`set` but does not overwrite the values of already
		existing keys.

		:param key: the key to set
		:param value: the value for the key
		:param timeout: the cache timeout for the key.
						If not specified, it uses the default timeout.
						If specified 0, it will never expire.
		:param noreply: instructs the server to not send the reply.
		:returns: Same as :meth:`set`, but also ``False`` for already
				  existing keys.
		:rtype: boolean
		"""
		return True

	def set_many(self, data, timeout=None, noreply=False):
		"""Sets multiple keys and values from a dict.

		:param data: a dict with the keys/values to set.
		:param timeout: the cache timeout for the key.
						If not specified, it uses the default timeout.
						If specified 0, it will never expire.
		:param noreply: instructs the server to not send the reply.
		:returns: Whether all given keys have been set.
		:rtype: boolean
		"""
		rv = True
		for key, value in _items(data):
			if not self.set(key, value, timeout):
				rv = False
		return rv

	def delete_many(self, keys, noreply=False):
		"""Deletes multiple keys at once.

		:param keys: The function accepts multiple keys as positional
					 arguments.
		:param noreply: instructs the server to not send the reply.
		:returns: Whether all given keys have been deleted.
		:rtype: boolean
		"""
		return all(self.delete(key) for key in keys)

	def clear(self):
		"""Clears the cache.  Keep in mind that not all caches support
		completely clearing the cache.
		:returns: Whether the cache has been cleared.
		:rtype: boolean
		"""
		return True

	def incr(self, key, delta=1, noreply=False):
		"""Increments the value of a key by `delta`.  If the key does
		not yet exist it is initialized with `delta`.

		For supporting caches this is an atomic operation.

		:param key: the key to increment.
		:param delta: the delta to add.
		:param noreply: instructs the server to not send the reply.
		:returns: The new value or ``None`` for backend errors.
		"""
		value = (self.get(key) or 0) + delta
		return value if self.set(key, value) else None

	def decr(self, key, delta=1, noreply=False):
		"""Decrements the value of a key by `delta`.  If the key does
		not yet exist it is initialized with `-delta`.

		For supporting caches this is an atomic operation.

		:param key: the key to increment.
		:param delta: the delta to subtract.
		:param noreply: instructs the server to not send the reply.
		:returns: The new value or `None` for backend errors.
		"""
		value = (self.get(key) or 0) - delta
		return value if self.set(key, value) else None

	def hgetall(self, key):
		"""Look up hash in the cache and return all fields and values for it.

		:param key: the key of hash to be looked up.
		:returns: The dict value of hash, if empty, return {}.
		:rtype: dict
		"""
		raise NotImplementedError()

	def hget(self, key, field):
		"""Look up field in the hash and return the value for it.

		:param key: the key of hash to be looked up.
		:param field: the filed in the hash to be looked up.
		:returns: The value if it exists and is readable, else ``None``.
		"""
		raise NotImplementedError()

	def hset(self, key, field, value, timeout=None, noreply=False):
		"""Add a new filed/value to the hash in cache (overwrites value, if key already
		exists in the cache).

		:param key: the key of hash to set
		:param key: the field in the hash to set
		:param value: the value for the field
		:param timeout: the cache timeout for the field.
						If not specified, it uses the default timeout.
						If specified 0, it will never expire.
		:param noreply: instructs the server to not send the reply.
		:returns: Whether the key existed and has been set.
		:rtype: boolean
		"""
		raise NotImplementedError()

	def hdel(self, key, field, noreply=False):
		"""Delete field of hash from the cache.

		:param key: the key of hash to delete
		:param key: the field in the hash to delete
		:param noreply: instructs the server to not send the reply.
		:returns: Whether the key has been deleted.
		:rtype: boolean
		"""
		raise NotImplementedError()

class NullCache(BaseCache):
	"""A cache that doesn't cache.  This can be useful for unit testing.

	:param default_timeout: a dummy parameter that is ignored but exists
							for API compatibility with other caches.
	"""
	def __init__(self, config):
		super(NullCache, self).__init__(config)


_test_memcached_key = re.compile(r'[^\x00-\x21\xff]{1,250}$').match
_PYLIBMC_BEHAVIORS = {
	'connect_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000,
	'send_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000 * 1000,
	'receive_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000 * 1000,
}

class MemcachedCache(BaseCache):
	"""A cache that uses memcached as backend.

	This cache looks into the following packages/modules to find bindings for
	memcached:

		- ``pylibmc``
		- ``google.appengine.api.memcached``
		- ``memcached``

	Implementation notes:  This cache backend works around some limitations in
	memcached to simplify the interface.  For example unicode keys are encoded
	to utf-8 on the fly.  Methods such as :meth:`~BaseCache.get_many` return
	the keys in the same format as passed.  Furthermore all get methods
	silently ignore key errors to not cause problems when untrusted user data
	is passed to the get methods which is often the case in web applications.

	:param host: memcached server host.
	:param port: memcached server port.
	:param default_timeout: the default timeout that is used if no timeout is
							specified on :meth:`~BaseCache.set`.
	:param key_prefix: a prefix that is added before all keys.  This makes it
					   possible to use the same memcached server for different
					   applications.  Keep in mind that
					   :meth:`~BaseCache.clear` will also clear keys with a
					   different prefix.
	"""

	def __init__(self, config):
		BaseCache.__init__(self, config)
		servers = ['%s:%d' % (config['host'], config['port'])]
		self.key_prefix = _to_native(config.get('key_prefix', None))
		if servers is None:
			servers = ['127.0.0.1:11211']
		self._client = self.import_preferred_memcache_lib(servers)
		if self._client is None:
			raise RuntimeError('no memcache module found')

	def _normalize_key(self, key):
		key = _to_native(key, 'utf-8')
		if self.key_prefix:
			key = self.key_prefix + key
		return key

	def _normalize_timeout(self, timeout):
		if timeout is None:
			timeout = self.default_timeout
		return timeout

	def get(self, key):
		key = self._normalize_key(key)
		# memcached doesn't support keys longer than that.  Because often
		# checks for so long keys can occur because it's tested from user
		# submitted data etc we fail silently for getting.
		if _test_memcached_key(key):
			return self._client.get(key)

	def get_many(self, keys):
		key_mapping = {}
		have_encoded_keys = False
		for key in keys:
			encoded_key = self._normalize_key(key)
			if not isinstance(key, str):
				have_encoded_keys = True
			if _test_memcached_key(key):
				key_mapping[encoded_key] = key
		d = rv = self._client.get_multi(key_mapping.keys())
		if have_encoded_keys or self.key_prefix:
			rv = {}
			for key, value in _iteritems(d):
				rv[key_mapping[key]] = value
		return rv

	def add(self, key, value, timeout=None, noreply=False):
		key = self._normalize_key(key)
		timeout = self._normalize_timeout(timeout)
		return self._client.add(key, value, timeout)

	def set(self, key, value, timeout=None, noreply=False):
		key = self._normalize_key(key)
		timeout = self._normalize_timeout(timeout)
		return self._client.set(key, value, timeout)

	def set_many(self, data, timeout=None, noreply=False):
		new_data = {}
		for key, value in _items(data):
			key = self._normalize_key(key)
			new_data[key] = value

		timeout = self._normalize_timeout(timeout)
		failed_keys = self._client.set_multi(new_data, timeout)
		return not failed_keys

	def delete(self, key, noreply=False):
		key = self._normalize_key(key)
		if _test_memcached_key(key):
			return self._client.delete(key) is not 0
		else:
			return False

	def delete_many(self, keys, noreply=False):
		new_keys = []
		for key in keys:
			key = self._normalize_key(key)
			if _test_memcached_key(key):
				new_keys.append(key)
		return self._client.delete_multi(new_keys) is not 0

	def clear(self):
		return self._client.flush_all()

	def incr(self, key, delta=1, noreply=False):
		key = self._normalize_key(key)
		return self._client.incr(key, delta)

	def decr(self, key, delta=1, noreply=False):
		key = self._normalize_key(key)
		return self._client.decr(key, delta)

	def import_preferred_memcache_lib(self, servers):
		"""Returns an initialized memcache client.  Used by the constructor."""
		try:
			import pylibmc
		except ImportError:
			pass
		else:
			return pylibmc.Client(servers, behaviors=_PYLIBMC_BEHAVIORS)

		try:
			import memcache
		except ImportError:
			pass
		else:
			return memcache.Client(servers)


try:

	from pymemcache.client import Client as PyMemcachedClient
	from pymemcache.serde import python_memcache_serializer, python_memcache_deserializer

	class PyMemcachedCache(BaseCache):
		"""A cache client based on pymemcache. implemented by pure python and support noreply.
		"""
		def __init__(self, config):
			BaseCache.__init__(self, config)
			self._client = PyMemcachedClient((config['host'], config['port']),
				serializer=python_memcache_serializer, deserializer=python_memcache_deserializer,
				connect_timeout=_DEFAULT_SOCKET_TIMEOUT, timeout=_DEFAULT_SOCKET_TIMEOUT,
				key_prefix=config.get('key_prefix', ''))

		def get(self, key):
			return self._client.get(key)

		def delete(self, key, noreply=False):
			self._client.delete(key, noreply)
			return True

		def get_many(self, keys):
			return self._client.get_many(keys)

		def set(self, key, value, timeout=None, noreply=False):
			if timeout is None:
				timeout = self.default_timeout
			return self._client.set(key, value, timeout, noreply)

		def add(self, key, value, timeout=None, noreply=False):
			if timeout is None:
				timeout = self.default_timeout
			return self._client.add(key, value, timeout, noreply)

		def set_many(self, data, timeout=None, noreply=False):
			if timeout is None:
				timeout = self.default_timeout
			return self._client.set_many(data, timeout, noreply)

		def delete_many(self, keys, noreply=False):
			return self._client.delete_many(keys, noreply)

		def clear(self):
			return self._client.flush_all(noreply=False)

		def incr(self, key, delta=1, noreply=False):
			return self._client.incr(key, delta, noreply)

		def decr(self, key, delta=1, noreply=False):
			return self._client.decr(key, delta, noreply)

except ImportError:

	PyMemcachedCache = MemcachedCache

_redis_kwargs_exclusions = set(('id', 'type', 'replica', 'default', 'default_timeout', 'key_prefix'))
_DEFAULT_REDIS_BLOCKING_POOL_TIMEOUT = 5

class RedisCache(BaseCache):
	"""Uses the Redis key-value store as a cache backend.

	:param host: address of the Redis server or an object which API is
				 compatible with the official Python Redis client (redis-py).
	:param port: port number on which Redis server listens for connections.
	:param password: password authentication for the Redis server.
	:param db: db (zero-based numeric index) on Redis Server to connect.
	:param default_timeout: the default timeout that is used if no timeout is
							specified on :meth:`~BaseCache.set`.
	:param key_prefix: A prefix that should be added to all keys.

	Any additional keyword arguments will be passed to ``redis.Redis``.
	"""

	def __init__(self, config):
		BaseCache.__init__(self, config)
		self.key_prefix = config.get('key_prefix', '')
		try:
			import redis
		except ImportError:
			raise RuntimeError('no redis module found')
		kwargs = dict((k, v) for k, v in config.items() if k not in _redis_kwargs_exclusions)
		if 'socket_timeout' not in kwargs:
			kwargs['socket_timeout'] = _DEFAULT_SOCKET_TIMEOUT
		if 'socket_connect_timeout' not in kwargs:
			kwargs['socket_connect_timeout'] = _DEFAULT_SOCKET_TIMEOUT
		if 'socket_keepalive' not in kwargs:
			kwargs['socket_keepalive'] = 1
		if 'socket_keepalive_options' not in kwargs:
			kwargs['socket_keepalive_options'] = _TCP_KEEP_ALIVE_OPTIONS
		if kwargs.pop('blocking_pool', False):
			if 'blocking_pool_timeout' in kwargs:
				kwargs['timeout'] = kwargs.pop('blocking_pool_timeout')
			else:
				kwargs['timeout'] = _DEFAULT_REDIS_BLOCKING_POOL_TIMEOUT
			connection_pool = redis.BlockingConnectionPool(**kwargs)
		else:
			connection_pool = redis.ConnectionPool(**kwargs)

		self._client = redis.Redis(connection_pool=connection_pool)

	def dump_object(self, value):
		"""Dumps an object into a string for redis.  By default it serializes
		integers as regular string and pickle dumps everything else.
		"""
		t = type(value)
		if t in _integer_types:
			return str(value).encode('ascii')
		return b'!' + pickle.dumps(value)

	def load_object(self, value):
		"""The reversal of :meth:`dump_object`.  This might be callde with
		None.
		"""
		if value is None:
			return None
		if value.startswith(b'!'):
			try:
				return pickle.loads(value[1:])
			except pickle.PickleError:
				return None
		try:
			return int(value)
		except ValueError:
			return value

	def get(self, key):
		return self.load_object(self._client.get(self.key_prefix + key))

	def get_list(self, keys):
		if self.key_prefix:
			keys = [self.key_prefix + key for key in keys]
		return [self.load_object(x) for x in self._client.mget(keys)]

	def get_many(self, keys):
		if self.key_prefix:
			query_keys = [self.key_prefix + key for key in keys]
		else:
			query_keys = keys
		values_list = self._client.mget(query_keys)
		values = {}
		for i in xrange(len(keys)):
			value = values_list[i]
			if value is not None:
				values[keys[i]] = self.load_object(value)
		return values

	def set(self, key, value, timeout=None, noreply=False):
		if timeout is None:
			timeout = self.default_timeout
		dump = self.dump_object(value)
		if timeout == 0:
			return self._client.set(name=self.key_prefix + key,value=dump)
		else:
			return self._client.setex(name=self.key_prefix + key, value=dump, time=timeout)

	def add(self, key, value, timeout=None, noreply=False):
		if timeout is None:
			timeout = self.default_timeout
		dump = self.dump_object(value)
		result = self._client.setnx(name=self.key_prefix + key, value=dump)
		if result and timeout != 0:
			result = self._client.expire(name=self.key_prefix + key, time=timeout)
		return result

	def set_many(self, data, timeout=None, noreply=False):
		if timeout is None:
			timeout = self.default_timeout
		pipe = self._client.pipeline()
		for key, value in _items(data):
			dump = self.dump_object(value)
			if timeout == 0:
				pipe.set(name=self.key_prefix + key, value=dump)
			else:
				pipe.setex(name=self.key_prefix + key, value=dump, time=timeout)
		return pipe.execute()

	def delete(self, key, noreply=False):
		self._client.delete(self.key_prefix + key)
		return True

	def delete_many(self, keys, noreply=False):
		if not keys:
			return True
		if self.key_prefix:
			keys = [self.key_prefix + key for key in keys]
		self._client.delete(*keys)
		return True

	def clear(self):
		status = False
		if self.key_prefix:
			keys = self._client.keys(self.key_prefix + '*')
			if keys:
				status = self._client.delete(*keys)
		else:
			status = self._client.flushdb()
		return status

	def incr(self, key, delta=1, noreply=False):
		return self._client.incr(name=self.key_prefix + key, amount=delta)

	def decr(self, key, delta=1, noreply=False):
		return self._client.decr(name=self.key_prefix + key, amount=delta)

	def hgetall(self, key):
		value = self._client.hgetall(self.key_prefix + key)
		if value is not None:
			for field in value:
				value[field] = self.load_object(value[field])
		return value

	def hget(self, key, field):
		return self.load_object(self._client.hget(self.key_prefix + key, field))

	def hset(self, key, field, value, timeout=None, noreply=False):
		self._client.hset(self.key_prefix + key, field, self.dump_object(value))
		return True

	def hdel(self, key, field, noreply=False):
		self._client.hdel(self.key_prefix + key, field)
		return True

class RawRedisCache(RedisCache):
	"""Same cache client as RedisCache, but only support string value.
	"""

	def __init__(self, config):
		RedisCache.__init__(self, config)

	def dump_object(self, value):
		if not isinstance(value, str):
			raise Exception('raw_redis_unsupport_value_type:' + str(type(value)))
		return value

	def load_object(self, value):
		return value

class _SsdbConnectionPool(object):

	def __init__(self, connection):
		self._connection = connection

	def get_connection(self, command_name, *keys, **options):
		return self._connection

	def release(self, connection):
		return

class SsdbCache(BaseCache):
	"""Uses the SSDB key-value store as a cache backend.

	:param host: address of the SSDB server.
	:param port: port number on which SSDB server listens for connections.
	:param default_timeout: the default timeout that is used if no timeout is
							specified on :meth:`~BaseCache.set`.
							defaults to 0, means never expire.
	:param key_prefix: A prefix that should be added to all keys.

	Any additional keyword arguments will be passed to ``ssdb.Connection``.
	"""

	def __init__(self, config):
		BaseCache.__init__(self, config)
		self.default_timeout = config.get('default_timeout', 0)
		self.key_prefix = config.get('key_prefix', '')
		try:
			import ssdb
			#patch ssdb
			import six
			import datetime
			ssdb.connection.iteritems = six.iteritems
			def expire(self, name, ttl):
				if isinstance(ttl, datetime.timedelta):
					ttl = ttl.seconds + ttl.days * 24 * 3600
				return self.execute_command('expire', name, ttl)
			ssdb.SSDB.expire = expire
		except ImportError:
			raise RuntimeError('no ssdb module found')
		kwargs = dict((k, v) for k, v in config.items() if k not in _redis_kwargs_exclusions)
		if 'socket_timeout' not in kwargs:
			kwargs['socket_timeout'] = _DEFAULT_SOCKET_TIMEOUT
		if 'socket_connect_timeout' not in kwargs:
			kwargs['socket_connect_timeout'] = _DEFAULT_SOCKET_TIMEOUT
		if 'socket_keepalive' not in kwargs:
			kwargs['socket_keepalive'] = 1
		if 'socket_keepalive_options':
			kwargs['socket_keepalive_options'] = _TCP_KEEP_ALIVE_OPTIONS
		connection_pool = _SsdbConnectionPool(ssdb.Connection(**kwargs))
		self._client = ssdb.SSDB(connection_pool=connection_pool)

	def get(self, key):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.get(key)

	def get_many(self, keys):
		if self.key_prefix:
			keys = [self.key_prefix + key for key in keys]
		values = self._client.multi_get(*keys)
		if self.key_prefix:
			values = dict([(key[len(self.key_prefix):], value) for key, value in values.iteritems()])
		return values

	def set(self, key, value, timeout=None, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		if timeout is None:
			timeout = self.default_timeout
		if timeout == 0:
			return self._client.set(key, value)
		else:
			return self._client.setx(key, value, timeout)

	def add(self, key, value, timeout=None, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		if timeout is None:
			timeout = self.default_timeout
		result = self._client.setnx(key, value)
		if result and timeout != 0:
			result = self._client.expire(key, timeout)
		return result

	def set_many(self, data, timeout=None, noreply=False):
		if timeout is None:
			timeout = self.default_timeout
		if self.key_prefix:
			new_data = {}
			for key in data:
				new_data[self.key_prefix + key] = data[key]
			data = new_data
		result = self._client.multi_set(**data)
		if result and timeout != 0:
			for key in data:
				result = self._client.expire(key, timeout)
		return result

	def delete(self, key, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		self._client.delete(key)
		return True

	def delete_many(self, keys, noreply=False):
		if not keys:
			return True
		if self.key_prefix:
			keys = [self.key_prefix + key for key in keys]
		self._client.multi_del(*keys)
		return True

	def clear(self):
		keys = self._client.keys('', '')
		if self.key_prefix:
			keys = [key for key in keys if key.startswith(self.key_prefix)]
		status = self._client.delete(*keys)
		return status

	def incr(self, key, delta=1, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.incr(self.key_prefix + key, delta)

	def decr(self, key, delta=1, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.decr(self.key_prefix + key, delta)

	def hgetall(self, key):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.hgetall(key)

	def hget(self, key, field):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.hget(key, field)

	def hset(self, key, field, value, timeout=None, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.hset(key, field, value)

	def hdel(self, key, field, noreply=False):
		if self.key_prefix:
			key = self.key_prefix + key
		return self._client.hdel(key, field)


class ReplicationCache(BaseCache):

	def __init__(self, config):
		BaseCache.__init__(self, config)
		self._client = []
		self._primary_cache = None
		self._id = config['id']
		primary = config.get('primary')
		caches = config['caches']
		for index, name in enumerate(caches.keys()):
			self._client.append(create_cache_client(name, caches[name]))
			if primary is not None and primary == name:
				self._primary_cache = self._client[index]

	def get_client(self):
		if self._primary_cache is not None:
			return self._primary_cache
		if len(self._client) == 0:
			return None
		return random.choice(self._client)

	def set(self, key, value, timeout=None, noreply=False):
		result = True
		for client in self._client:
			if not client.set(key, value, timeout, noreply):
				result = False
		return result

	def add(self, key, value, timeout=None, noreply=False):
		result = True
		for client in self._client:
			if not client.add(key, value, timeout, noreply):
				result = False
		return result

	def set_many(self, data, timeout=None, noreply=False):
		result = True
		for client in self._client:
			if not client.set_many(data, timeout, noreply):
				result = False
		return result

	def get(self, key):
		client = self.get_client()
		if client is None:
			return None
		return client.get(key)

	def get_list(self, keys):
		client = self.get_client()
		if client is None:
			return None
		return client.get_list(keys)

	def get_many(self, keys):
		client = self.get_client()
		if client is None:
			return None
		return client.get_many(keys)

	def delete(self, key, noreply=False):
		result = True
		for client in self._client:
			if not client.delete(key, noreply):
				result = False
		return result

	def delete_many(self, keys, noreply=False):
		result = True
		for client in self._client:
			if not client.delete_many(keys, noreply):
				result = False
		return result

	def clear(self):
		result = True
		for client in self._client:
			if not client.clear():
				result = False
		return result

	def incr(self, key, delta=1, noreply=False):
		result = None
		for client in self._client:
			result = client.incr(key, delta, noreply)
			if result is None:
				return None
		return result

	def decr(self, key, delta=1, noreply=False):
		result = None
		for client in self._client:
			result = client.decr(key, delta, noreply)
			if result is None:
				return None
		return result

	def hgetall(self, key):
		client = self.get_client()
		if client is None:
			return None
		return client.hgetall(key)

	def hget(self, key, field):
		client = self.get_client()
		if client is None:
			return None
		return client.hget(key, field)

	def hset(self, key, field, value, timeout=None, noreply=False):
		result = True
		for client in self._client:
			if not client.hset(key, field, value, timeout, noreply):
				result = False
		return result

	def hdel(self, key, field, noreply=False):
		result = True
		for client in self._client:
			if not client.hdel(key, field, noreply):
				result = False
		return result


class DistributionCache(BaseCache):
	"""A cache distributior based on cache key.

	:param method: support conhash / mod / div. default is conhash
		conhash: children should have property replica, default replica is 32
		mod: require key_regex, factor. client_id = key % factor
		div: require key_regex, factor. client_id = key / factor
	:param key_regex: regex used to extract base key from cache key to caculate client_id.
		the first regex group matched in cache key will be used as base key.
	:param factor: divisor in mod or div method..
	"""

	def __init__(self, config):
		BaseCache.__init__(self, config)
		self._client = {}
		self._id = config['id']
		self._method = config.get('method')
		if self._method == 'mod' or self._method == 'div':
			self._init_method_mod_div(config)
		else:
			self._init_method_conhash(config)

	def _init_method_conhash(self, config):
		self._conhash = ConHash()
		caches = config['caches']
		for index, name in enumerate(caches.keys()):
			self._client[name] = create_cache_client(name, caches[name])
			self._conhash.add_node(str(name), caches[name].get('replica', 32), index)
		self.get_client_by_key = self._get_client_by_key_conhash

	def _get_client_by_key_conhash(self, key):
		client_id = self._conhash.lookup(str(key))
		return self._client.get(client_id)

	def _init_method_mod_div(self, config):
		self._regex = re.compile(config['key_regex'])
		self._factor = config['factor']
		caches = config['caches']
		for name in caches:
			self._client[int(name)] = create_cache_client(name, caches[name])
		if self._method == 'mod':
			self.get_client_by_key = self._get_client_by_key_mod
		else:
			self.get_client_by_key = self._get_client_by_key_div

	def _get_client_by_key_mod(self, key):
		try:
			client_id = int(self._regex.match(key).group(1)) % self._factor
		except:
			return None
		return self._client.get(client_id)

	def _get_client_by_key_div(self, key):
		try:
			client_id = int(self._regex.match(key).group(1)) / self._factor
		except:
			return None
		return self._client.get(client_id)

	def set(self, key, value, timeout=None, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return False
		return client.set(key, value, timeout, noreply)

	def add(self, key, value, timeout=None, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return False
		return client.add(key, value, timeout, noreply)

	def set_many(self, data, timeout=None, noreply=False):
		query_table = defaultdict(dict)
		for key in data:
			client = self.get_client_by_key(key)
			if client is None:
				return False
			query_table[client][key] = data[key]
		for client, query in query_table.iteritems():
			if not client.set_many(query, timeout, noreply):
				return False
		return True

	def get(self, key):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.get(key)

	def get_many(self, keys):
		query_table = defaultdict(list)
		for key in keys:
			client = self.get_client_by_key(key)
			if client is None:
				return None
			query_table[client].append(key)
		values = {}
		for client, query in query_table.iteritems():
			result = client.get_many(query)
			if result is None:
				return None
			values.update(result)
		return values

	def delete(self, key, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return False
		return client.delete(key, noreply)

	def delete_many(self, keys, noreply=False):
		query_table = defaultdict(list)
		for key in keys:
			client = self.get_client_by_key(key)
			if client is None:
				return False
			query_table[client].append(key)
		for client, query in query_table.iteritems():
			if not client.delete_many(query, noreply):
				return False
		return True

	def clear(self):
		result = True
		for client in self._client.values():
			if not client.clear():
				result = False
		return result

	def incr(self, key, delta=1, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.incr(key, delta, noreply)

	def decr(self, key, delta=1, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.decr(key, delta, noreply)

	def hgetall(self, key):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.hgetall(key)

	def hget(self, key, field):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.hget(key, field)

	def hset(self, key, field, value, timeout=None, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.hset(key, field, value, timeout, noreply)

	def hdel(self, key, field, noreply=False):
		client = self.get_client_by_key(key)
		if client is None:
			return None
		return client.hdel(key, field, noreply)


_cache_classes = {
	'null': NullCache,
	'memcached': MemcachedCache,
	'pymemcached': PyMemcachedCache,
	'redis': RedisCache,
	'rawredis': RawRedisCache,
	'ssdb': SsdbCache,
	'replication': ReplicationCache,
	'distribution': DistributionCache
}

def create_cache_client(cache_id, config):
	cache_type = config['type']
	config['id'] = cache_id
	if cache_type not in _cache_classes:
		raise RuntimeError('unknown_cache_type: %s' % cache_type)
	return _cache_classes[cache_type](config)

_cache_clients = {}
_default_client = None

def init_cache(config):
	""" init cache clients
	:param config:
	{
		'main':  {
			'type': 'memcached',
			'host': '127.0.0.1',
			'port': 11211,
			'default_timeout': 7 * 24 * 60 * 60,
			'key_prefix': 'test.',
			'default': True,
		},
		'test':  {
			'type': 'redis',
			'host': '127.0.0.1',
			'port': 6379,
			'default_timeout': 7 * 24 * 60 * 60,
			'key_prefix': 'test.',
		},
		'null':  {
			'type': 'null',
		},
		'replication': {
			'type': 'replication',
			'primary': 'replication.main',
			'caches': {
				'replication.main': {
					'type': 'distribution',
					'caches': {
						'distribution.1': {
							'type': 'memcached',
							'host': '127.0.0.1',
							'port': 11211,
							'default_timeout': 7 * 24 * 60 * 60,
							'key_prefix': 'test',
							'replica': 32
						},
						'distribution.2': {
							'type': 'memcached',
							'host': '127.0.0.1',
							'port': 11212,
							'default_timeout': 7 * 24 * 60 * 60,
							'key_prefix': 'test',
							'replica': 32
						}
					}
				},
				'replication.bak': {
					'type': 'distribution',
					'method': 'mod',
					'key_regex': r'\w+\.(\d+)',
					'factor': 2,
					'caches': {
						'0': {
							'type': 'memcached',
							'host': '127.0.0.1',
							'port': 11211,
							'default_timeout': 7 * 24 * 60 * 60,
							'key_prefix': 'test'
						},
						'1': {
							'type': 'memcached',
							'host': '127.0.0.1',
							'port': 11212,
							'default_timeout': 7 * 24 * 60 * 60,
							'key_prefix': 'test'
						}
					}
				}
			}
		}
	}
	null: default_timeout
	memcached: host, port, default_timeout, key_prefix
	pymemcached: host, port, default_timeout, key_prefix. this client supports noreply.
	redis: host, port, password, db, default_timeout, key_prefix
	rawredis: host, port, password, db, default_timeout, key_prefix
	ssdb: host, port, default_timeout, key_prefix
	distribution: caches, method, key_regex, factor
	replication: caches, primary
	there should be one instance with default=True
	:return: None
	"""
	global _cache_clients
	global _default_client
	for name in config:
		item = config[name]
		client = create_cache_client(name, item)
		_cache_clients[name] = client
		if item.get('default'):
			_default_client = client

def get_cache(name=None):
	if name is None:
		return _default_client
	return _cache_clients.get(name)

def get(key):
	return _default_client.get(key)

def delete(key, noreply=False):
	return _default_client.delete(key, noreply)

def get_list(keys):
	return _default_client.get_list(keys)

def get_many(keys):
	return _default_client.get_many(keys)

def set(key, value, timeout=None, noreply=False):
	return _default_client.set(key, value, timeout, noreply)

def add(key, value, timeout=None, noreply=False):
	return _default_client.add(key, value, timeout, noreply)

def set_many(data, timeout=None, noreply=False):
	return _default_client.set_many(data, timeout, noreply)

def delete_many(keys, noreply=False):
	return _default_client.delete_many(keys, noreply)

def clear():
	return _default_client.clear()

def incr(key, delta=1, noreply=False):
	return _default_client.incr(key, delta, noreply)

def decr(key, delta=1, noreply=False):
	return _default_client.decr(key, delta, noreply)

def hgetall(key):
	return _default_client.hgetall(key)

def hget(key, field):
	return _default_client.hget(key, field)

def hset(key, field, value, timeout=None, noreply=False):
	return _default_client.hset(key, field, value, timeout, noreply)

def hdel(key, field, noreply=False):
	return _default_client.hdel(key, field, noreply)
