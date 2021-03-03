import logging
import sys

from optparse import OptionParser

from lib.util import Utilities
from dspace_reports.item_indexer import ItemIndexer


class RunItemIndexer():
    def __init__(self, config=None, logger=None):
        if config is None:
            print("ERROR: A configuration file required to create the stats indexer.")
            sys.exit(1)

        self.config = config
        self.solr_server = config['solr_server']

        # Set up logging
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger('dspace-reports')

    def run(self):
        # Create items stats indexer
        item_indexer = ItemIndexer(config=self.config, logger=self.logger)
        
        # Index items stats from Solr
        item_indexer.index()


def main():
    parser = OptionParser()

    parser.add_option("-c", "--config", dest="config_file", default="config/application.yml", help="Configuration file")
    parser.add_option("-o", "--output_dir", dest="output_dir", help="Directory for results files.")

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
        print("ERROR: Unable to load configuration.")
        sys.exit(1)

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
        logger.error("Output directory does not exist.")
        sys.exit(1)

    # Create stats indexer
    indexer = RunItemIndexer(config=config, logger=logger)
    
    # Get item statistics from Solr
    indexer.run()


if __name__ == "__main__":
    main()