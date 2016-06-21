def init_config(config):
	if isinstance(config, (str, unicode)):
		with open(config, 'r') as f:
			content = f.read()
		import json
		config_dict = json.loads(content)
	elif isinstance(config, dict):
		config_dict = config
	else:
		config_dict = config.__dict__
	globals().update(config_dict)
