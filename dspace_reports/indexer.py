import logging
import sys
from datetime import date, datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

from lib.database import Database
from lib.api import DSpaceRestApi
from lib.solr import DSpaceSolr

class Indexer(object):
    def __init__(self, config):
        if config is None:
            print('A configuration file required to create the stats indexer.')
            return

        self.config = config
        self.base_url = config['dspace_server'] + '/handle/'
        self.solr_server = config['solr_server']

        self.logger = logging.getLogger('dspace-reports')

        # Create REST API object
        self.rest = DSpaceRestApi(rest_server=config['rest_server'])
        if self.rest is None:
            self.logger.error("Unable to create Indexer due to earlier failures creating a connection to the REST API.")
            sys.exit(1)

        # Create Solr server object
        self.solr = DSpaceSolr(solr_server=config['solr_server'])
        if self.solr is None:
            self.logger.error("Unable to create Indexer due to earlier failures creating a connection to Solr.")
            sys.exit(1)

        # The time periods used to generate statistical reports
        self.time_periods = ['month', 'year', 'all']

    def get_date_range(self, time_period=None):
        date_range = str()
        if time_period is None:
            return date_range

        if time_period == 'month':
            self.logger.info("Getting stats for last month.")
            dt = date.today()
            today = datetime.combine(dt, datetime.max.time()).isoformat() + 'Z'
            self.logger.debug("Current date: %s ", today)
            one_month_ago = datetime.combine((date.today() + relativedelta(months=-1)), datetime.min.time()).isoformat() + 'Z'
            self.logger.debug("One month ago: %s ", one_month_ago)

            date_range = f"time:[{one_month_ago} TO {today}]"
        elif time_period == 'year':
            self.logger.info("Getting stats for current year.")
            dt = date.today()
            today = datetime.combine(dt, datetime.max.time()).isoformat() + 'Z'
            self.logger.debug("Current date: %s ", today)
            
            first_day_of_year = datetime.combine((date(date.today().year, 1, 1)), datetime.min.time()).isoformat() + 'Z'
            self.logger.debug("First day of year: %s ", first_day_of_year)

            date_range = f"time:[{first_day_of_year} TO {today}]"

        return date_range