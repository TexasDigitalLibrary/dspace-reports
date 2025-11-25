"""Class for interacting with a DSpace 7+ Solr instance"""

import logging
import re
import requests


class DSpaceSolr():
    """Class for interacting with a DSpace 7+ Solr instance"""

    def __init__(self, config):
        self.config = config

        # URL to Solr instance
        solr_url = self.config['url']
        self.url = self.configure_url(solr_url)

        # Timeout in seconds for requests to Solr
        self.connection_timeout = self.config['connection_timeout']

        # Define Solr search path
        self.solr_search_path = "/search/select"

        # Define Solr statistics path
        self.solr_statistics_path = "/statistics/select"

        # Create session
        self.session = requests.Session()
        self.request_headers = {'Content-type': 'application/json'}

        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to Solr:  %s.", self.url)
        self.test_connection()

    def configure_url(self, url = ''):
        """Configure Solr URL"""

        if (url is None or len(url) == 0):
            return ''

        # Ensure solr_server has trailing slash
        if url[len(url)-1] != '/':
            return url + '/'

        return url

    def test_connection(self):
        """Test Solr connection"""

        self.logger.debug("Testing Solr server connection to URL: %s.", self.url)
        response = self.session.get(self.url, headers=self.request_headers,
                                    timeout=self.connection_timeout)

        if response.status_code == 200:
            self.logger.debug("Solr server connection successful.")
            return True

        self.logger.warning("Solr server connection failed.")
        return None

    def construct_url(self, command, params=None):
        """Create Solr URL"""

        if params is None:
            params = {}

        parameters = ''
        first = True
        for key, value in params.items():
            if first:
                parameters += '?' + key + '=' + str(value)
                first = False
            else:
                parameters += '&' + key + '=' + str(value)

        new_url = self.url + command + parameters
        return new_url

    def call(self, call_type='GET', path=None, params=None):
        """Make call to Solr server"""

        if path is None:
            return None

        if params is None:
            params = {}

        # Construct URL with Solr URL and the path
        url = self.url + path

        if call_type == 'POST':
            try:
                response = self.session.post(url, params=params, headers=self.request_headers,
                                            timeout=self.connection_timeout)
            except requests.exceptions.Timeout:
                self.logger.error("Call to Solr timed out after %s seconds.", 
                                  str(self.connection_timeout))
        else:
            try:
                response = self.session.get(url, params=params,headers=self.request_headers,
                                            timeout=self.connection_timeout)
            except requests.exceptions.Timeout:
                self.logger.error("Call to Solr timed out after %s seconds.", 
                                  str(self.connection_timeout))

        return response

    def query_search(self, params=None):
        """Query Solr search core"""

        query_search_url = self.url + self.solr_search_path
        return self.call(path=query_search_url, params=params)

    def query_statistics(self, params=None):
        """Query Solr statistics core"""

        query_statistics_url = self.url + self.solr_statistics_path
        return self.call(path=query_statistics_url, params=params)

    def get_statistics_shards(self):
        """Get Solr shards with statistics"""

        # Vars
        shards = str()
        shards = f"{self.url}statistics"
        statistics_core_years = []

        # URL for Solr status to check active cores
        solr_query_params = {"action": "STATUS", "wt": "json"}
        solr_cores_url = self.url + "admin/cores"
        self.logger.debug("Solr cores URL: %s", solr_cores_url)
        shards_response = self.session.get(solr_cores_url, params=solr_query_params,
                                           headers=self.request_headers,
                                           timeout=self.connection_timeout)

        if shards_response.status_code == 200:
            data = shards_response.json()

            # Iterate over active cores from Solr's STATUS response
            for core in data["status"]:
                # Pattern to match, for example: statistics-2018
                pattern = re.compile("^statistics-[0-9]{4}$")

                if not pattern.match(core):
                    continue

                # Append current core to list
                self.logger.debug("Adding Solr core: %s", core)
                statistics_core_years.append(core)

        if len(statistics_core_years) > 0:
            for core in statistics_core_years:
                shards += f",{self.solr_server}{core}"

        self.logger.info("Using these shards to search for statistics: %s", shards)
        return shards

    def get_solr_server(self):
        """Return reference to Solr server"""

        return self.url
