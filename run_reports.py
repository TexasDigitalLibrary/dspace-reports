"""Class for running all statistics indexers and optionally emailing the results"""

import argparse
from datetime import datetime
import logging
import sys

from psycopg import sql

from database_manager import DatabaseManager
from lib.database import Database
from lib.emailer import Emailer
from lib.output import Output
from lib.util import Utilities


class RunReports():
    """Class for running all statistics indexers and optionally emailing the results"""

    def __init__(self, config=None, output_dir=None, send_email=False, logger=None):
        if config is None:
            print('A configuration file required to generate stats reports.')
            return

        if output_dir is None:
            print('Must specify an output directory.')
            return

        # Vars
        self.config = config
        self.output_dir = output_dir
        self.send_email = send_email

        # Create output object
        self.output = Output(config=config)

         # Create email object
        self.emailer = Emailer(config=config)

        # Set up logging
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger('dspace-reports')

    def run(self):
        """Run reports"""

        self.logger.info("Begin running all reports.")
        # Stats reports to create
        reports = [
            {
                'name': 'repository',
                'table': 'repository_stats',
                'orderBy': 'repository_name'
            },
            {
                'name': 'communities',
                'table': 'community_stats',
                'orderBy': 'parent_community_name'
            },
            {
                'name': 'collections',
                'table': 'collection_stats',
                'orderBy': 'parent_community_name'
            },
            {
                'name': 'items',
                'table': 'item_stats',
                'orderBy': 'collection_name'
            }
        ]

        # Create CSV files of each stats report
        csv_report_files = []
        for report in reports:
            csv_report_file = self.create_csv_report(report=report)
            self.logger.info("Created CSV report file: %s.", csv_report_file)

            # Convert column names to human readable text
            csv_report_files.append(csv_report_file)

        # Create Excel report file from CSV files
        self.logger.info("Creating Excel file with CSV report files.")
        excel_report_file = self.create_excel_report(csv_report_files)

        if self.config['create_zip_archive']:
            # Create ZIP archive with Excel report file
            self.logger.info("Creating ZIP archive with Excel report file.")
            zip_report_archive = self.create_zip_archive(excel_report_file)

            # Email ZIP archive with Excel file to email address list in the configuration
            self.logger.info("Emailing Excel file to admin addresses in the configuration.")
            if self.send_email and self.emailer and zip_report_archive:
                self.logger.info("Emailing report to address list in configuration.")
                self.emailer.email_report_admins(report_file_path=zip_report_archive)
        else:
            # Email Excel file to email address list in the configuration
            self.logger.info("Emailing Excel file to admin addresses in the configuration.")
            if self.send_email and self.emailer and excel_report_file:
                self.logger.info("Emailing report to address list in configuration.")
                self.emailer.email_report_admins(report_file_path=excel_report_file)

        self.logger.info("Finished running all reports.")

    def create_csv_report(self, report=None):
        """Create CSV report"""

        if report is None:
            self.logger.error("Must specify a report.")
            return None

        # Vars
        column_names = []
        data = []

        self.logger.debug("Creating CSV file for report %s...", report['table'])

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.debug(cursor.mogrify(sql.SQL("SELECT * FROM {} ORDER BY {} ASC").format(sql.Identifier(report['table']), sql.Identifier(report['orderBy'],))))
                cursor.execute(sql.SQL("SELECT * FROM {} ORDER BY {} ASC").format(sql.Identifier(report['table']), sql.Identifier(report['orderBy'],)))

                desc = cursor.description
                column_names = [col[0] for col in desc]
                self.logger.debug("Report has %s columns.", str(len(column_names)))
                data = [dict(zip(column_names, row))
                        for row in cursor.fetchall()]

        self.logger.debug("Report has %s rows.", str(len(data)))

        # Save raw database table in a CSV file
        report_csv_file = self.output.save_report_csv_file(
            output_file_path=self.output_dir + report['name'] + '.csv',
            headers=column_names, data=data)

        # Convert column names to human readable text based on mappings in DatabaseManager
        column_names_new = self.map_column_names(report_name=report['name'],
                                                 column_names=column_names)
        report_csv_file = self.output.update_header_report_csv_file(
            input_file_path=report_csv_file, headers_old=column_names,
            headers_new=column_names_new)

        return report_csv_file

    def create_excel_report(self, csv_report_files=None):
        """Create Excel report"""

        if csv_report_files is None or len(csv_report_files) == 0:
            self.logger.warning("No CSV files to create Excel file.")
            return False

        # Combine CSV files into single Excel file
        output_file_path = self.output_dir + datetime.now().strftime('dspace-reports_%Y-%m-%d_%H-%M-%S.xlsx')
        excel_report_file = self.output.save_report_excel_file(
            output_file_path=output_file_path, worksheet_files=csv_report_files)
        if excel_report_file:
            self.logger.info("Finished saving Excel file to %s.",
                             excel_report_file)
            return excel_report_file

        self.logger.error("There was an error saving the Excel file.")
        return False

    def create_zip_archive(self, excel_report_file=None):
        """Create ZIP file"""

        if excel_report_file is None:
            self.logger.warning("No Excel file to create ZIP archive.")
            return False

        # Create ZIP archvie with the Excel file
        output_file_path = self.output_dir + datetime.now().strftime('dspace-reports_%Y-%m-%d_%H-%M-%S.zip')
        zip_report_archive = self.output.save_report_zip_archive(output_file_path=output_file_path,
                                                                 excel_report_file=excel_report_file
                                                                 )
        if zip_report_archive:
            self.logger.info("Finished saving ZIP archive to %s.",
                             zip_report_archive)
            return zip_report_archive

        self.logger.error("There was an error saving the ZIP archive.")
        return False

    def map_column_names(self, report_name=None, column_names=None):
        """Map column names"""

        if report_name is None or column_names is None:
            self.logger.error("One or more parameters missing to map table columns.")
            return False

        column_map = None
        if report_name == 'repository':
            column_map = DatabaseManager.repository_column_map
        elif report_name == 'communities':
            column_map = DatabaseManager.communities_column_map
        elif report_name == 'collections':
            column_map = DatabaseManager.collections_column_map
        elif report_name == 'items':
            column_map = DatabaseManager.items_column_map
        else:
            self.logger.error('Unrecognized report name.')

        if column_map is not None:
            for i, column_name in enumerate(column_names):
                self.logger.debug("Looking at column name: %s.", column_names[i])
                if column_name in column_map:
                    self.logger.debug("Changing column name to %s.", column_map[column_name])
                    column_names[i] = column_map[column_name]

        return column_names


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

    # Store email parameter
    send_email = args.send_email

    # Create reports generator
    reports = RunReports(config=config, output_dir=output_dir, send_email=send_email, logger=logger)

    # Generate stats reports from database
    reports.run()


if __name__ == "__main__":
    main()
