"""Class for interacting with a DSpace 7+ Solr instance"""

import logging
import re
import requests


class DSpaceSolr():
    """Class for interacting with a DSpace 7+ Solr instance"""

    def __init__(self, solr_server=None):
        # Ensure solr_server has trailing slash
        if solr_server[len(solr_server)-1] != '/':
            self.solr_server = solr_server + '/'
        else:
            self.solr_server = solr_server

        # Timeout in seconds for requests to Solr
        self.timeout = 120

        # Create session
        self.session = requests.Session()
        self.request_headers = {'Content-type': 'application/json'}

        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to DSpace REST API:  %s.", self.solr_server)
        self.test_connection()

    def test_connection(self):
        """Test Solr connection"""

        self.logger.debug("Testing Solr server connection: %s.", self.solr_server)
        response = self.session.get(self.solr_server, headers=self.request_headers,
                                    timeout=self.timeout)

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

        new_url = self.solr_server + command + parameters
        return new_url

    def call(self, call_type='GET', url=None, params=None):
        """Make call to Solr server"""

        if url is None:
            return None

        if params is None:
            params = {}

        if call_type == 'POST':
            try:
                response = self.session.post(url, params=params, headers=self.request_headers,
                                            timeout=self.timeout)
            except requests.exceptions.Timeout:
                self.logger.error("Call to Solr timed out after %s seconds.", str(self.timeout))
        else:
            try:
                response = self.session.get(url, params=params,headers=self.request_headers,
                                            timeout=self.timeout)
            except requests.exceptions.Timeout:
                self.logger.error("Call to Solr timed out after %s seconds.", str(self.timeout))

        return response

    def get_statistics_shards(self):
        """Get Solr shards with statistics"""

        # Vars
        shards = str()
        shards = f"{self.solr_server}statistics"
        statistics_core_years = []

        # URL for Solr status to check active cores
        solr_query_params = {"action": "STATUS", "wt": "json"}
        solr_url = self.solr_server + "admin/cores"
        self.logger.debug("Solr cores URL: %s", solr_url)
        shards_response = self.session.get(solr_url, params=solr_query_params,
                                           headers=self.request_headers, timeout=self.timeout)

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

        return self.solr_server
