import struct
import socket
import calendar
import pytz
import time
from datetime import datetime
import dateutil.parser

def ip_to_int(addr):
	return struct.unpack("!I", socket.inet_aton(addr))[0]

ip2int = ip_to_int	#Deprecated!

def int_to_ip(addr):
	return socket.inet_ntoa(struct.pack("!I", addr))

int2ip = int_to_ip	#Deprecated!

def timestamp_to_string(timestamp):
	return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')

time2string = timestamp_to_string	#Deprecated!

def string_to_timestamp(dt_string):
	return datetime_to_timestamp(dateutil.parser.parse(dt_string))

def get_tz(country_code):
	try:
		return pytz.timezone(pytz.country_timezones(country_code)[0])
	except:
		return pytz.timezone('Asia/Singapore')

def datetime_to_timestamp(dt):
	return calendar.timegm(dt.utctimetuple())

datetime_to_int = datetime_to_timestamp	#Deprecated!

def timestamp_to_datetime(timestamp, country_code):
	return datetime.fromtimestamp(timestamp, get_tz(country_code))

datetime_from_timestamp = timestamp_to_datetime

def get_current_datetime(country_code):
	now = int(time.time())
	return timestamp_to_datetime(now, country_code)

def strptime_with_tz(dt_string, dt_format, country_code):
	dt = datetime.strptime(dt_string, dt_format)
	return get_tz(country_code).localize(dt)
