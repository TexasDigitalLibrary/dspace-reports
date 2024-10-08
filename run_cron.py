"""Script for running all statistics indexers"""

import argparse
import sys

from lib.util import Utilities
from run_indexer import RunIndexer
from run_reports import RunReports


def main():
    """Main function"""

    parser = argparse.ArgumentParser(
                    prog='Database Manager',
                    description='Commands to manage statistics database tables')

    parser.add_argument("-c", "--config", dest="config_file", action='store', type=str,
                        default="config/application.yml", help="Configuration file")
    parser.add_argument("-o", "--output_dir", dest="output_dir", action='store', type=str,
                        help="Directory for results files.")
    parser.add_argument("-e", "--send_email", dest="send_email",
                        action='store_true',
                        help="Send email with stats reports?")

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
        print("Unable to load configuration.")
        sys.exit(0)

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
        sys.exit(0)

    # Store send email parameter
    send_email = args.send_email

    # Create stats indexer
    indexer = RunIndexer(config=config, logger=logger)

    # Get item statistics from Solr
    indexer.run()

    # Create reports generator
    reports = RunReports(config=config, output_dir=output_dir, send_email=send_email, logger=logger)

    # Generate stats reports from database
    reports.run()


if __name__ == "__main__":
    main()
