"""Class for indexing community statistics"""

import argparse
import logging
import sys

from lib.util import Utilities
from dspace_reports.community_indexer import CommunityIndexer


class RunCommunityIndexer():
    """Class for indexing community statistics"""

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
        """Function to run community indexer"""

        # Create communities stats indexer
        community_indexer = CommunityIndexer(config=self.config, logger=self.logger)

        # Index communities stats from Solr
        community_indexer.index()


def main():
    """Main function"""

    parser = argparse.ArgumentParser(
                    prog='Database Manager',
                    description='Commands to manage statistics database tables')

    parser.add_argument("-c", "--config", dest="config_file", action='store', type=str,
                        default="config/application.yml", help="Configuration file")
    parser.add_argument("-o", "--output_dir", dest="output_dir", action='store', type=str,
                        help="Directory for results files.")

    args = parser.parse_args()

    # Create utilities object
    utilities = Utilities()

    # Check required options fields
    if args.output_dir is None:
        parser.print_help()
        parser.error("Must specify an output directory.")

    # Load config
    print("Loading configuration from file: %s", args.config_file)
    config = utilities.load_config(args.config_file)
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
    output_dir = args.output_dir
    if output_dir[len(output_dir)-1] != '/':
        output_dir = output_dir + '/'

    # Ensure output_dir exists
    output_dir_exists = utilities.ensure_directory_exists(output_dir)
    if output_dir_exists is False:
        logger.error("Output directory does not exist.")
        sys.exit(1)

    # Create stats indexer
    indexer = RunCommunityIndexer(config=config, logger=logger)

    # Get item statistics from Solr
    indexer.run()

if __name__ == "__main__":
    main()
