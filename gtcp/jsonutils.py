try:
	import simplejson as json
except:
	import json

def to_json(data, ensure_ascii=False):
	return json.dumps(data, ensure_ascii=ensure_ascii, separators=(',', ':'))

def from_json(s):
	return json.loads(s)

def from_json_safe(s):
	try:
		return json.loads(s)
	except:
		return None

_slash_escape = '\\/' in to_json('/')

def to_json_html_safe(obj, **kwargs):
	rv = to_json(obj, **kwargs)	\
		.replace(u'<', u'\\u003c')	\
		.replace(u'>', u'\\u003e')	\
		.replace(u'&', u'\\u0026')	\
		.replace(u"'", u'\\u0027')	\
		.replace(u'\u2028', u'\\u2028')	\
		.replace(u'\u2029', u'\\u2029')
	if _slash_escape:
		rv = rv.replace('\\/', '/')
	return rv
