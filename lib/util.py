import logging
import os
import yaml

class Utilities(object):
    def load_config(self, config_file=None):
        if config_file is None:
            print("Must specify a configuration file.")
            return False
        
        config = {}
        path = config_file

        if not path or not os.path.isfile(path):
            print('Configuration file is missing.')
            return False

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        return config

    def load_logger(self, config=None):
        if config is None:
            print('No configuration given, cannot create logger.')
            return False

        # Set variables
        log_path = config['log_path'] or 'logs/'
        if log_path[len(log_path)-1] != '/':
            log_path = log_path + '/'

        log_file = config['log_file'] or 'dspace-reports.log'

        log_level_string = config['log_level'] or 'INFO'
        if log_level_string == 'INFO':
            log_level = logging.INFO
        elif log_level_string == 'DEBUG':
            log_level = logging.DEBUG
        elif log_level_string == 'WARNING':
            log_level = logging.WARNING
        elif log_level_string == 'ERROR':
            log_level = logging.ERROR
        else:
            log_level = logging.INFO

        # Create logger
        logger = logging.getLogger('dspace-reports')
        logger.setLevel(log_level)
        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

        file_handler = logging.FileHandler("{0}/{1}".format(log_path, log_file))
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)

        return logger

    def ensure_directory_exists(self, output_file_path=None):
        if output_file_path is None:
            print("Must specify an output file.")
            return False

        directory = os.path.dirname(output_file_path)

        if os.path.isdir(directory) and os.path.exists(directory):
            return True
        else:
            os.mkdir(directory)
            return True