import logging
import os
import psycopg2.extras
import re
import requests
import sys
import yaml

from optparse import OptionParser

from database_manager import DatabaseManager
from lib.api import DSpaceRestApi
from lib.util import Utilities
from dspace_reports.collection_indexer import CollectionIndexer


class RunCollectionIndexer():
    def __init__(self, config=None):
        if config is None:
            print('A configuration file required to create the stats indexer.')
            return

        self.config = config
        self.solr_server = config['solr_server']

        self.logger = logging.getLogger('dspace-reports')

        # Create REST API object
        self.api = DSpaceRestApi(rest_server=config['rest_server'])

        # The time periods used to generate statistical reports
        periods = ['month', 'year', 'all']

    def run(self):
        # Create database manager
        database_manager = DatabaseManager(config=self.config)

        # Create collections stats indexer
        collection_indexer = CollectionIndexer(config=self.config)
        
        # Index collections stats from Solr
        collection_indexer.index()


def main():
    parser = OptionParser()

    parser.add_option("-c", "--config", dest="config_file", default="config/application.yml", help="Configuration file")
    parser.add_option("-o", "--output_dir", dest="output_dir", help="Directory for results files.")
    parser.add_option("-e", "--email", action="store_true", dest="email", default=False, help="Email to receive reports")

    (options, args) = parser.parse_args()

    # Create utilities object
    utilities = Utilities()

    # Check required options fields
    if options.output_dir is None:
        parser.print_help()
        parser.error("Must specify an output directory.")

    # Load config
    print("Loading configuration from file: %s", options.config_file)
    config = utilities.load_config(options.config_file)
    if not config:
        print("Unable to load configuration.")
        sys.exit(0)

    # Set up logging
    logger = utilities.load_logger(config=config)

    # Ensure work_dir has trailing slash
    work_dir = config['work_dir']
    if work_dir[len(work_dir)-1] != '/':
        work_dir = work_dir + '/'

    # Ensure output_dir has trailing slash
    output_dir = options.output_dir
    if output_dir[len(output_dir)-1] != '/':
        output_dir = output_dir + '/'

    # Ensure output_dir exists
    output_dir_exists = utilities.ensure_directory_exists(output_dir)
    if output_dir_exists is False:
        sys.exit(0)

    # Create database manager
    database_manager = DatabaseManager(config=config)

    # Create stats indexer
    indexer = RunCollectionIndexer(config=config)
    
    # Get item statistics from Solr
    indexer.run()


if __name__ == "__main__":
    main()