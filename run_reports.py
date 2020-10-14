import itertools
import logging
import os
import psycopg2.extras
import re
import requests
import sys
import yaml

from optparse import OptionParser
from datetime import datetime

from lib.database import Database
from lib.email import Email
from lib.output import Output
from lib.util import Utilities


class RunReports():
    def __init__(self, config=None, output_dir=None, email=False):
        if config is None:
            print('A configuration file required to generate stats reports.')
            return

        if output_dir is None:
            print('Must specify an output directory.')
            return

        # Vars
        self.config = config
        self.output_dir = output_dir
        self.email = email

        # Create output object
        self.output = Output(config=config)
        
         # Create email object
        self.email = Email(config=config)

        # Setup logging
        self.logger = logging.getLogger('dspace-reports')

    def run(self): 
        # Stats reports to create
        reports = [
            {
                'name': 'communities',
                'table': 'community_stats'
            },
            {
                'name': 'collections',
                'table': 'collection_stats'
            },
            {
                'name': 'items',
                'table': 'item_stats'
            }
        ]

        # Create CSV files of each stats report
        csv_report_files = []
        for report in reports:
            csv_report_file = self.create_csv_report(report=report)
            self.logger.info("Created CSV report file: {csv_report_file}.".format(csv_report_file=csv_report_file))
            csv_report_files.append(csv_report_file)

        # Create Excel report file from CSV files
        self.logger.info("Creating Excel file with CSV report files.")
        excel_report_file = self.create_excel_report(csv_report_files)

        # Email Excel file to email address list in the configuration
        self.logger.info("Emailing Excel file to admin addresses in the configuration.")
        if excel_report_file and self.email:
            self.logger.info("Emailing report to address list in configuration.")
            self.email.email_report_admins(report_file_path=excel_report_file)

    def create_csv_report(self, report=None):
        if report is None:
            self.logger.error("Must specify a report.")
            return

        # Vars
        column_names = []
        data = []

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                print(cursor.mogrify("SELECT * FROM %s" %(report['table'])))
                cursor.execute("SELECT * FROM %s" %(report['table']))
                        
                desc = cursor.description
                column_names = [col[0] for col in desc]
                data = [dict(zip(column_names, row))  
                        for row in cursor.fetchall()]
           
        report_csv_file = self.output.save_report_csv_file(output_file_path=self.output_dir + report['name'] + '.csv', headers=column_names, data=data)            
        return report_csv_file

    def create_excel_report(self, csv_report_files=None):
        if csv_report_files is None or len(csv_report_files) == 0:
            self.logger.warn("No CSV files to create Excel file.")
            return False

        # Combine CSV files into single Excel file
        output_file_path = self.output_dir + datetime.now().strftime('dspace-reports_%Y-%m-%d_%H-%M-%S.xlsx')
        excel_report_file = self.output.save_report_excel_file(output_file_path=output_file_path, worksheet_files=csv_report_files)
        if excel_report_file:
            self.logger.info('Finished saving Excel file to {excel_report_file}.'.format(excel_report_file=excel_report_file))
            return excel_report_file
        else:
            self.logger.error("There was an error saving the Excel file.")
            return False


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
    print("Loading configuration from file: %s" %(options.config_file))
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

    # Store email parameter
    email = options.email

    # Create stats indexer
    reports = RunReports(config=config, output_dir=output_dir, email=email)
    
    # Get item statistics from Solr
    reports.run()


if __name__ == "__main__":
    main()