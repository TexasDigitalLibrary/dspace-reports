import requests
import logging
import sys

from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET


class DSpaceRestApi(object):
    def __init__(self, rest_server=None):
        # Ensure rest_server has trailing slash
        if rest_server[len(rest_server)-1] != '/':
            self.rest_server = rest_server + '/'
        else:
            self.rest_server = rest_server

        self.limit = 100
        self.headers = {'Accept': 'application/json'}
        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to DSpace REST API:  %s.", self.rest_server)
        self.test_connection()

    def test_connection(self):
        url = self.rest_server + 'status'
        self.logger.debug("Testing REST API connection: %s.", url)
        response = requests.get(url)
        status = ET.fromstring(response.content)
        okay = status.find('okay')
        if okay is not None and okay.text == 'true':
            self.logger.debug("REST API connection successful")
            return True
        else:
            self.logger.warning("REST API connection NOT successful")
            return False

    def construct_url(self, command, params={}):
        parameters = ''
        first = True
        for key, value in params.items():
            if first:
                parameters += '?' + key + '=' + str(value)
                first = False
            else:
                parameters += '&' + key + '=' + str(value)

        new_url = self.rest_server + command + parameters
        return new_url

    def rest_call(self, type='GET', url=''):
        if type == 'POST':
            response = requests.put(url, headers=self.headers)
        else:
            response = requests.get(url, headers=self.headers)

        self.logger.debug(url)
        response_json = response.json()
        self.logger.debug(response_json)
        return response_json
        
    def get_hierarchy(self):
        hierarchy_url = self.construct_url(command = 'hierarchy')
        hierarchy = self.rest_call(url = hierarchy_url)
        return hierarchy

    def get_communities(self):
        communities_url = self.construct_url(command = 'communities')
        communities = self.rest_call(url = communities_url)
        return communities

    def get_top_communities(self):
        top_communities_url = self.construct_url(command = 'communities/top-communities')
        top_communities = self.rest_call(url = top_communities_url)
        return top_communities

    def get_community(self, community_id=None):
        if community_id is None:
            return
        community_url = self.construct_url(command = f"communities/{community_id}")
        community = self.rest_call(url = community_url)
        return community

    def get_collection_items(self, collection_id=None):
        if collection_id is None:
            return
        items_url = self.construct_url(command = f"collections/{collection_id}/items")
        items = self.rest_call(url = items_url)
        return items
        
    def get_items(self):
        offset = 0
        params = {}
        all_items = []

        while True:
            params = {'offset': offset, 'limit': self.limit}
            items_url = self.construct_url(command = 'items', params = params)
            items = self.rest_call(url = items_url)
            if len(items) == 0:
                break

            all_items = all_items + items
            offset = offset + self.limit

        return all_items

    def get_item(self, item_id=None):
        if item_id is None:
            return
        item_url = self.construct_url(command = f"items/{item_id}")
        item = self.rest_call(url = item_url)
        return item