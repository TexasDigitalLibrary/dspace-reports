"""Base indexer class"""

import logging
import sys

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from lib.api import DSpaceRestApi
from lib.solr import DSpaceSolr

class Indexer():
    """Base indexer class"""

    def __init__(self, config=None, logger=None):
        if config is None:
            print("ERROR: A configuration file required to create the stats indexer.")
            sys.exit(1)

        self.config = config
        self.base_url = config['dspace_server'] + '/handle/'
        self.solr_server = config['solr_server']

        # Set up logging
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger('dspace-reports')

        # Create REST API object
        self.rest = DSpaceRestApi(rest_server=config['rest_server'])
        if self.rest is None:
            self.logger.error("Unable to create Indexer due to earlier failures creating a " +
                              "connection to the REST API.")
            sys.exit(1)

        # Create Solr server object
        self.solr = DSpaceSolr(solr_server=config['solr_server'])
        if self.solr is None:
            self.logger.error("Unable to create Indexer due to earlier failures creating a " +
                              "connection to Solr.")
            sys.exit(1)

        # The time periods used to generate statistical reports
        self.time_periods = ['month', 'year', 'all']

    def index(self):
        """Index function"""

        self.logger.info("Base indexing class.")

    def get_date_range(self, time_period=None):
        """Calculate date range"""

        date_range = []
        if time_period is None:
            self.logger.debug("time_period of none given to get_date_range() method.")
            return date_range

        if time_period == 'month':
            self.logger.info("Getting stats for last month.")
            dt = date.today()
            today = datetime.combine(dt, datetime.min.time()).isoformat() + 'Z'
            self.logger.debug("Current date: %s ", today)
            one_month_ago = datetime.combine(
                (date.today() + relativedelta(months=-1)), datetime.min.time()).isoformat() + 'Z'
            self.logger.debug("One month ago: %s ", one_month_ago)

            date_range = [one_month_ago, today]
        elif time_period == 'year':
            self.logger.info("Getting stats for current academic year.")
            dt = date.today()
            today = datetime.combine(dt, datetime.max.time()).isoformat() + 'Z'
            self.logger.debug("Current date: %s ", today)

            current_month = datetime.today().month
            if current_month <= 9:
                fiscal_year = datetime.today().year - 1
            else:
                fiscal_year = datetime.today().year

            first_day_of_academic_year = datetime.combine(
                (date(fiscal_year, 9, 1)), datetime.min.time()).isoformat() + 'Z'
            self.logger.debug("First day of academic year: %s ", first_day_of_academic_year)

            date_range = [first_day_of_academic_year, today]
        elif time_period == 'all':
            self.logger.info("Getting stats for all time.")
            dt = date.today()
            today = datetime.combine(dt, datetime.max.time()).isoformat() + 'Z'
            self.logger.debug("Current date: %s ", today)

            date_range = ['*', today]

        self.logger.debug("Date range has %s dates.", len(date_range))
        return date_range
