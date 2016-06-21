'''
Common logger library

[Config]
log_dir: log files directory. default './log'
sentry_dsn: sentry dsn. default no sentry
rollover_when: rollover interval. default 'MIDNIGHT'
	S - Seconds
	M - Minutes
	H - Hours
	D - Days
	MIDNIGHT - roll over at midnight
rollover_backup_count: how many backup log files are kept. default 30
	if rollover_backup_count = 0, all log files are kept.
	if rollover_backup_count > 0, when rollover is done, no more than rollover_backup_count files are kept - the oldest ones are deleted.

[Normal Python Program]
# config.py
# Add LOGGER_CONFIG
LOGGER_CONFIG = {
	'log_dir': './log',
	'sentry_dsn': 'http://xxxxxxxx',
}
#if no config.py, will use './log' as log_dir

[Integrate Django]
# settings.py
# Add LOGGER_CONFIG
LOGGER_CONFIG = {
	'log_dir': './log',
	'sentry_dsn': 'http://xxxxxxxx',
	'sentry_project_release': '1860da72a8c86bf4832a598410186ae668bbaf20',  # used by sentry to track which git release does error belong to
}
# Add 'raven.contrib.django.raven_compat' in INSTALLED_APPS
INSTALLED_APPS = (
	...,
	'raven.contrib.django.raven_compat',
)
# Add 'raven.contrib.django.middleware.SentryLogMiddleware' in MIDDLEWARE_CLASSES
MIDDLEWARE_CLASSES = (
	...,
	'raven.contrib.django.middleware.SentryLogMiddleware',
)
'''

log = None

try:
	from django.utils.log import AdminEmailHandler
	
	class ErrorEmailHandler(AdminEmailHandler):
		def format_subject(self, subject):
			pos_list = filter(lambda x:x>0, [subject.find(c) for c in '|\r\n'])
			if pos_list:
					return subject[:min(pos_list)]
			else:
					return subject
except:
	pass

def _log_record_exception(func):
	def _func(self):
		try:
			return func(self)
		except:
			log.exception('log_exception|thread=%s:%s,file=%s:%s,func=%s:%s,log=%s',
				self.process, self.thread, self.filename, self.lineno, self.module, self.funcName, self.msg)
			raise
	return _func

def append_exc(func):
	def _append_exc(*args,**kwargs):
		if 'exc_info' not in kwargs:
			kwargs['exc_info'] = True
		return func(*args,**kwargs)
	return _append_exc
	
def init_logger(log_dir=None, sentry_dsn=None, sentry_project_release=None, rollover_when='MIDNIGHT', rollover_backup_count=30):

	if log_dir is None:
		log_dir = './log'
	
	import os
	import sys
	log_dir = os.path.abspath(log_dir)
	if log_dir and not os.path.exists(log_dir):
		os.mkdir(log_dir)

	logger_config = {
		'version': 1,
		'disable_existing_loggers': True,
		'formatters': {  
			'standard': {
				'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(filename)s:%(lineno)d|%(module)s.%(funcName)s|%(message)s',
				'datefmt': '%Y-%m-%d %H:%M:%S',
			},
			'short' : {
				'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(message)s',
				'datefmt': '%Y-%m-%d %H:%M:%S',
			},
			'data' : {
				'format': '%(asctime)s.%(msecs)03d|%(message)s',
				'datefmt': '%Y-%m-%d %H:%M:%S',
			},
		},
		'handlers': {
			'file_fatal': {
				'level':'CRITICAL',
				'class':'gtcp.loggingmp.MPTimedRotatingFileHandler',
				'filename': os.path.join(log_dir, 'fatal.log').replace('\\','/'),
				'when': rollover_when,
				'backupCount': rollover_backup_count,
				'formatter':'standard',
			},
			'file_error': {
				'level':'WARNING',
				'class':'gtcp.loggingmp.MPTimedRotatingFileHandler',
				'filename': os.path.join(log_dir, 'error.log').replace('\\','/'),
				'when': rollover_when,
				'backupCount': rollover_backup_count,
				'formatter':'standard',
			},
			'file_info': {
				'level':'DEBUG',
				'class':'gtcp.loggingmp.MPTimedRotatingFileHandler',
				'filename': os.path.join(log_dir, 'info.log').replace('\\','/'),
				'when': rollover_when,
				'backupCount': rollover_backup_count,
				'formatter':'short',
			},
			'file_data': {
				'level':'DEBUG',
				'class':'gtcp.loggingmp.MPTimedRotatingFileHandler',
				'filename': os.path.join(log_dir, 'data.log').replace('\\','/'),
				'when': rollover_when,
				'backupCount': rollover_backup_count,
				'formatter':'data',
			},
		},
		'loggers': {
			'main': {
				'handlers': ['file_fatal', 'file_error', 'file_info'],
				'level': 'DEBUG',
				'propagate': True,
			},
			'data': {
				'handlers': ['file_data'],
				'level': 'DEBUG',
				'propagate': True,
			},
			'django.request': {
				'handlers': ['file_fatal', 'file_error', 'file_info'],
				'level': 'ERROR',
				'propagate': True,
			},
			'tornado.access': {
				'handlers': ['file_data'],
				'level': 'DEBUG',
				'propagate': True,
			},
			'tornado.application': {
				'handlers': ['file_fatal', 'file_error', 'file_info'],
				'level': 'DEBUG',
				'propagate': True,
			},
			'tornado.general': {
				'handlers': ['file_fatal', 'file_error', 'file_info'],
				'level': 'DEBUG',
				'propagate': True,
			},
		}
	}

	is_django_app = False
	is_debug = False
	is_test = False
	try:
		from django.conf import settings
		is_django_app = settings.configured
		if is_django_app:
			is_debug = settings.DEBUG
			is_test = 'TEST' in dir(settings) and settings.TEST
	except:
		pass
	if not is_django_app:
		import config
		is_debug = 'DEBUG' in dir(config) and config.DEBUG
		is_test = 'TEST' in dir(config) and config.TEST

	if is_debug:
		logger_config['handlers']['file_debug'] = {
			'level':'DEBUG',
			'class':'gtcp.loggingmp.MPTimedRotatingFileHandler',
			'filename': os.path.join(log_dir, 'debug.log').replace('\\','/'),
			'when': rollover_when,
			'backupCount': rollover_backup_count,
			'formatter':'short',
		}
		logger_config['loggers']['django.db.backends'] = {
			'handlers': ['file_debug'],
			'level': 'DEBUG',
			'propagate': True,
		}
	elif not is_test:
		loggers = logger_config['loggers']
		for logger_item in loggers:
			if loggers[logger_item]['level'] == 'DEBUG':
				loggers[logger_item]['level'] = 'INFO'

	if not is_debug and sentry_dsn is not None:
		try:
			import raven
			if is_django_app:
				logger_config['handlers']['sentry'] = {
					'level':'WARNING',
					'class':'raven.contrib.django.raven_compat.handlers.SentryHandler',
					'formatter':'short',
				}
				settings.RAVEN_CONFIG = {'dsn': sentry_dsn, 'release': sentry_project_release}
			else:
				logger_config['handlers']['sentry'] = {
					'level':'WARNING',
					'class':'raven.handlers.logging.SentryHandler',
					'dsn': sentry_dsn,
					#'auto_log_stacks': True,
					'formatter':'short',
					'release': sentry_project_release,
				}
			logger_config['loggers']['django.request']['handlers'].append('sentry')
			logger_config['loggers']['main']['handlers'].append('sentry')
		except:
			pass

	work_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
	recover_path = False
	if work_dir not in sys.path:
		sys.path.append(work_dir)
		recover_path = True

	import logging
	try:
		import logging.config
		logging.config.dictConfig(logger_config)
	except:
		import loggerconfig
		loggerconfig.dictConfig(logger_config)

	if recover_path:
		sys.path.remove(work_dir)

	global log
	log = logging.getLogger('main')
	log.exception = append_exc(log.error)
	log.assertion = log.critical
	log.data = logging.getLogger('data').info
	logging.LogRecord.getMessage = _log_record_exception(logging.LogRecord.getMessage)

#try init log
def try_init_logger():
	try:
		from django.conf import settings
	
		setting_keys = dir(settings)
		if 'LOGGER_CONFIG' in setting_keys:
			init_logger(**settings.LOGGER_CONFIG)
		elif 'LOGGING' in setting_keys and settings.LOGGING:
			import logging
			global log
			log = logging.getLogger('main')
			log.exception = append_exc(log.error)
			log.data = logging.getLogger('data').info
		else:
			init_logger()
	except:
		try:
			import config
			init_logger(**config.LOGGER_CONFIG)
		except:
			try:
				init_logger()
			except:
				pass

if log is None:
	try_init_logger()
