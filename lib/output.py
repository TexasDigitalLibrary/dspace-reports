"""Class for saving stats reports to CSV and Excel files"""

import csv
import os
import logging
import shutil
from zipfile import ZIP_DEFLATED, ZipFile
import xlsxwriter
from lib.util import Utilities


class Output():
    """Class for saving stats reports to CSV and Excel files"""

    def __init__(self, config=None):
        self.config = config
        self.logger = logging.getLogger('dataverse-reports')
        self.utilities = Utilities()

        # Ensure work_dir has trailing slash
        self.work_dir = config['work_dir']
        if self.work_dir[len(self.work_dir)-1] != '/':
            self.work_dir = self.work_dir + '/'

    def save_report_csv_file(self, output_file_path=None, headers=None, data=None):
        """Save stats report to CSV file"""

        if headers is None:
            headers = []

        if data is None:
            data = []

        # Sanity checks
        if output_file_path is None:
            self.logger.error("Output file path is required.")
            return False
        if not headers:
            self.logger.error("Report headers are required.")
            return False
        if not self.utilities.ensure_directory_exists(output_file_path):
            self.logger.error("Output directory doesn't exist and can't be created.")
            return False

        # TODO: make this configurable
        if "repository_id" in headers:
            headers.remove("repository_id")

        with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction='ignore',
                                    dialect='excel', quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for result in data:
                writer.writerow(result)

        self.logger.info("Saved report to CSV file %s.", output_file_path)
        return output_file_path

    def update_header_report_csv_file(self, input_file_path=None, headers_old=None, 
                                      headers_new=None):
        """Update headers of CSV file"""

        # Sanity checks
        if input_file_path is None:
            self.logger.error("Input file path is required.")
            return False
        if not headers_old:
            self.logger.error("Old report headers are required.")
            return False
        if not headers_new:
            self.logger.error("New report headers are required.")
            return False
        if not self.utilities.check_file_exists(input_file_path):
            self.logger.error("Input file doesn't exist.")
            return False

        temp_csv_file_path = self.work_dir + 'temp.csv'

        with open(input_file_path, 'r', encoding="utf-8") as fp:
            reader = csv.DictReader(fp, fieldnames=headers_new)

            with open(temp_csv_file_path, 'w', newline='', encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=reader.fieldnames)
                writer.writeheader()
                header_mapping = next(reader)
                writer.writerows(reader)

        # Copy tempfile to report csv file
        destination_file_path = shutil.copyfile(temp_csv_file_path, input_file_path)
        return destination_file_path

    def save_report_excel_file(self, output_file_path=None, worksheet_files=None):
        """"Save stats report to Excel file"""

        if worksheet_files is None:
            worksheet_files = []

        # Sanity checks
        if output_file_path is None:
            self.logger.error("Output file path is required.")
            return False
        if len(worksheet_files) == 0:
            self.logger.error("Worksheets files list is empty.")
            return False
        if not self.utilities.ensure_directory_exists(output_file_path):
            self.logger.error("Output directory doesn't exist and can't be created.")
            return False

        # Create Excel workbook
        self.logger.info("Creating Excel file: %s", output_file_path)
        workbook = xlsxwriter.Workbook(output_file_path, {'strings_to_numbers': True})

        # Add worksheet(s)
        for worksheet_file in worksheet_files:
            # Get worksheet title from filename
            filename_w_ext = os.path.basename(worksheet_file)
            filename, file_extension = os.path.splitext(filename_w_ext)
            filename_parts = filename.split("-")
            if len(filename_parts) == 2:
                workbook_name = filename_parts[1]
            else:
                workbook_name = filename

            worksheet = workbook.add_worksheet(workbook_name)
            worksheet.freeze_panes(1, 0)
            with open(worksheet_file, 'rt', encoding='utf8') as f:
                reader = csv.reader(f)
                for r, row in enumerate(reader):
                    for c, col in enumerate(row):
                        worksheet.write(r, c, col)

        workbook.close()

        self.logger.info("Saved report to Excel file %s.", output_file_path)
        return output_file_path

    def save_report_zip_archive(self, output_file_path=None, excel_report_file=None):
        """"Save stats report to zip file"""

        # Sanity checks
        if output_file_path is None:
            self.logger.error("Output file path is required.")
            return False
        if excel_report_file is None:
            self.logger.error("Excel report file is required.")
            return False
        if not self.utilities.ensure_directory_exists(output_file_path):
            self.logger.error("Output directory doesn't exist and can't be created.")
            return False

        # Store Excel report file in the root of the archive
        archive_name = os.path.basename(excel_report_file)

        self.logger.debug("Creating ZIP archive: %s", output_file_path)
        with ZipFile(file=output_file_path, mode='w', compression=ZIP_DEFLATED) as report_zip:
            report_zip.write(filename=excel_report_file, arcname=archive_name)

        self.logger.info("Excel report file saved to ZIP archive: %s", output_file_path)
        return output_file_path
