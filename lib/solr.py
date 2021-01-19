import requests
import logging
import re


class DSpaceSolr(object):
    def __init__(self, solr_server=None):
        # Ensure solr_server has trailing slash
        if solr_server[len(solr_server)-1] != '/':
            self.solr_server = solr_server + '/'
        else:
            self.solr_server = solr_server

        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to DSpace REST API:  %s.", self.solr_server)
        self.test_connection()

    def test_connection(self):
        url = self.solr_server
        self.logger.debug("Testing Solr server connection: %s.", url)
        response = requests.get(url)

        if response.status_code == requests.codes.ok:
            self.logger.debug("Solr server connection successful")
            return True
        else:
            self.logger.warning("Solr server connection NOT successful")
            return None

    def construct_url(self, command, params={}):
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

    def call(self, type='GET', url=None, params={}):
        if url is None:
            return

        if type == 'POST':
            response = requests.put(url, params=params)
        else:
            response = requests.get(url, params=params)

        return response

    def get_statistics_shards(self, time_period):
        # Vars
        shards = str()
        shards = f"{self.solr_server}statistics"
        statistics_core_years = []

        # URL for Solr status to check active cores
        solr_query_params = {"action": "STATUS", "wt": "json"}
        solr_url = self.solr_server + "/admin/cores"
        response = requests.get(solr_url, params=solr_query_params)

        if response.status_code == requests.codes.ok:
            data = response.json()

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
        return self.solr_server