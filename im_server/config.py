LOGGER_CONFIG = {
	'log_dir': './log/',
}

import os
from gtcp.config import init_config

config_file = 'config/config.json'

init_config(os.path.join(os.path.dirname(__file__), config_file))

from gtcp.config import *