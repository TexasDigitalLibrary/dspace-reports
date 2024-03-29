import logging
import sys

from optparse import OptionParser

from lib.util import Utilities
from dspace_reports.repository_indexer import RepositoryIndexer
from dspace_reports.community_indexer import CommunityIndexer
from dspace_reports.collection_indexer import CollectionIndexer
from dspace_reports.item_indexer import ItemIndexer


class RunIndexer():
    def __init__(self, config=None, logger=None):
        if config is None:
            print('A configuration file required to create the stats indexer.')
            sys.exit(1)

        self.config = config
        self.solr_server = config['solr_server']

        # Set up logging
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger('dspace-reports')

    def run(self):
        self.logger.info("Begin running all indexing.")

        # Create items stats indexer
        repository_indexer = RepositoryIndexer(config=self.config, logger=self.logger)
        
        # Index repository stats from Solr
        repository_indexer.index()

        # Create communities stats indexer
        community_indexer = CommunityIndexer(config=self.config, logger=self.logger)
        
        # Index communities stats from Solr
        community_indexer.index()

        # Create collections stats indexer
        collection_indexer = CollectionIndexer(config=self.config, logger=self.logger)
        
        # Index collections stats from Solr
        collection_indexer.index()

        # Create items stats indexer
        item_indexer = ItemIndexer(config=self.config, logger=self.logger)
        
        # Index items stats from Solr
        item_indexer.index()

        self.logger.info("Finished running all indexing.")

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
    indexer = RunIndexer(config=config, logger=logger)
    
    # Get item statistics from Solr
    indexer.run()
    

if __name__ == "__main__":
    main()