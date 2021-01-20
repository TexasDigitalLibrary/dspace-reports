import logging
import sys

from optparse import OptionParser

from lib.util import Utilities
from dspace_reports.repository_indexer import RepositoryIndexer


class RunRepositoryIndexer():
    def __init__(self, config=None):
        if config is None:
            print('A configuration file required to create the stats indexer.')
            return

        self.config = config
        self.solr_server = config['solr_server']

        self.logger = logging.getLogger('dspace-reports')

    def run(self):
        # Create items stats indexer
        repository_indexer = RepositoryIndexer(config=self.config)
        
        # Index repository stats from Solr
        repository_indexer.index()


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

    # Create stats indexer
    indexer = RunRepositoryIndexer(config=config)
    
    # Get item statistics from Solr
    indexer.run()


if __name__ == "__main__":
    main()