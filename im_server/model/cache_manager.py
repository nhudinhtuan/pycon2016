from gtcp import cache
from gtcp import config

SESSION_USERNAME_PREFIX = 's.u.%s'
SESSION_CLIENT_ID_PREFIX = 's.e.%s'
SESSION_CACHE_NAME = 'session'


if hasattr(config, 'CACHE_SERVERS'):
	cache.init_cache(config.CACHE_SERVERS)

def get_client_id(username):
	return cache.get_cache(SESSION_CACHE_NAME).get(SESSION_USERNAME_PREFIX % username)

def get_client_id_list(username_list):
	client_id_dict = cache.get_cache(SESSION_CACHE_NAME).get_many([(SESSION_USERNAME_PREFIX % username) for username in username_list])
	result = {}
	if client_id_dict:
		for cache_key, client_id in client_id_dict.iteritems():
			result[cache_key[4:]] = client_id
	return result

def get_username(client_id):
	return cache.get_cache(SESSION_CACHE_NAME).get(SESSION_CLIENT_ID_PREFIX % client_id)

def set_username(username, client_id, expiry_time=0):
	cache_client = cache.get_cache(SESSION_CACHE_NAME)
	return cache_client.set(SESSION_USERNAME_PREFIX % username, client_id, expiry_time) and cache_client.set(SESSION_CLIENT_ID_PREFIX % client_id, username, expiry_time)

def remove_client_id(client_id):
	return cache.get_cache(SESSION_CACHE_NAME).delete(SESSION_CLIENT_ID_PREFIX % client_id)

def remove_username(username):
	return cache.get_cache(SESSION_CACHE_NAME).delete(SESSION_USERNAME_PREFIX % username)