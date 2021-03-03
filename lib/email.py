import os
import smtplib
import logging

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


class Email(object):
    def __init__(self, config=None):
        self.config = config
        self.logger = logging.getLogger('dspace-reports')

    def email_report_admins(self, report_file_path=None):
        if report_file_path is None:
            self.logger.error("A report file path and admin email address must be specified.")
            return False

        # Construct email information
        subject = 'DSpace statistical reports for {name}'.format(name=self.config['dspace_name']) 
        from_email = self.config['from_email']
        body = "The report in Excel format is attached."

        # Send email(s) to contact(s)
        for admin_email in self.config['admin_emails']:
            self.logger.info('Sending report to {admin_email}.'.format(admin_email=admin_email))
            self.__email_report_internal(report_file_path=report_file_path, to_email=admin_email, from_email=from_email, subject=subject, body=body)

    def __email_report_internal(self, report_file_path=None, to_email=None, from_email=None, subject=None, body=None):
        if report_file_path is None:
            self.logger.error("A report file path is required.")
            return False
        if to_email is None or from_email is None or subject is None or body is None:
            self.logger.error("One or more required email addresses is missing.")
            return False

        # Create message with text fields
        message = MIMEMultipart()
        message['Subject'] = subject
        message['To'] = to_email
        message['From'] = from_email
        message.attach(MIMEText(body, 'plain'))

        # Check that report file exists
        if not os.path.isfile(report_file_path):
            self.logger.warning("Report file doesn't exist: %s.", report_file_path)
            return False

        # Attach report file(s)
        path, report_file_name = os.path.split(report_file_path)
        attachment = open(report_file_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % report_file_name)
        message.attach(part)

        # Get SMTP configuration 
        smtp_host = self.config['smtp_host']
        smtp_auth = self.config['smtp_auth']
        smtp_port = self.config['smtp_port']
        smtp_username = self.config['smtp_username']
        smtp_password = self.config['smtp_password']

        # Send email
        self.logger.info('Sending DSpace report to {email}.'.format(email=to_email))
        server = smtplib.SMTP(smtp_host, smtp_port)
        if smtp_auth == 'tls':
            server.starttls()
        if smtp_username and smtp_password:
            server.login(smtp_username, smtp_password)
        server.send_message(message)
        server.quit()
