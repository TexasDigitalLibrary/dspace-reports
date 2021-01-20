import logging
import sys

from optparse import OptionParser

from lib.util import Utilities
from run_indexer import RunIndexer
from run_reports import RunReports


class RunCron():
    def __init__(self, config=None):
        if config is None:
            print('A configuration file required to create the stats indexer.')
            return

        self.config = config
        self.solr_server = config['solr_server']

        self.logger = logging.getLogger('dspace-reports')

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
        
    # Store email parameter
    email = options.email

    # Create stats indexer
    indexer = RunIndexer(config=config)
    
    # Get item statistics from Solr
    indexer.run()

    # Create reports generator
    reports = RunReports(config=config, output_dir=output_dir, email=email)
    
    # Generate stats reports from database
    reports.run()
    

if __name__ == "__main__":
    main()