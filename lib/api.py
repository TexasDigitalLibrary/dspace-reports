import requests
import logging

import xml.etree.ElementTree as ET


class DSpaceRestApi(object):
    def __init__(self, rest_server=None):
        # Ensure URL of rest_server has trailing slash
        url = rest_server['url']
        if url[len(url)-1] != '/':
            self.url = url + '/'
        else:
            self.url = url

        self.username = rest_server['username']
        self.password = rest_server['password']
        self.session_id = None

        self.limit = 100
        self.headers = {'Accept': 'application/json'}
        self.cookies = {}

        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to DSpace REST API:  %s.", self.url)
        
        # Authenticate using parameters in configuration
        authenticated = self.authenticate()
        if authenticated is False:
            return None

        # Test connection to REST API
        self.test_connection()

    def authenticate(self):
        self.logger.info("Authenticating connection to REST API")
       
       # Create data dictionary
        data = {'email':self.username, 'password':self.password}
        
        # Attempt to log in to REST API
        login_url = self.construct_url(command = 'login')
        login_response = requests.post(login_url, headers=self.headers, data=data)
        self.logger.info("Calling REST API: %s" %(login_response.url))

        if login_response.status_code == 200:
            self.logger.info("Successfully authenticated: %s" %(str(login_response.status_code)))
            self.logger.info(login_response.cookies)
            if 'JSESSIONID' in login_response.cookies:
                self.logger.info("Received session ID: %s" %(login_response.cookies['JSESSIONID']))
                self.session_id = login_response.cookies['JSESSIONID']
                self.cookies = {'JSESSIONID':self.session_id}
                return True
            else:
                self.logger.info("No session ID in response.")
                return False
        else:
            self.logger.info("REST API authentication failed: %s" %(str(login_response.status_code)))
            self.logger.info(login_response.text)
            return False

    def test_connection(self):
        if self.session_id is None:
            self.logger.error("Must authenticate before connecting to the REST API.")
            return False

        connection_url = self.url + 'status'
        self.logger.info("Testing REST API connection: %s.", connection_url)
        response = requests.get(connection_url)
        
        status = ET.fromstring(response.content)
        okay = status.find('okay')
        if okay is not None and okay.text == 'true':
            self.logger.info("REST API connection successful.")
            return True
        else:
            self.logger.error("REST API connection NOT successful.")
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

        final_url = self.url + command + parameters
        return final_url

    def rest_call(self, type='GET', url='', headers=None, data={}):
        if headers is None:
            headers = self.headers

        if type == 'POST':
            response = requests.post(url, headers=headers, cookies=self.cookies, data=data)
        else:
            response = requests.get(url, headers=headers, cookies=self.cookies)

        self.logger.debug(response.status_code)
        self.logger.debug(response.text)
        response_json = response.json()
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
        
    def get_items(self, expand=[]):
        offset = 0
        params = {}
        expandValue = ''
        all_items = []

        if len(expand) > 0:
            expandValue = ','.join(expand)
            params['expand'] = expandValue
            self.logger.debug("Added expand list to parameters: %s " %(expandValue))

        while True:
            self.logger.debug("Retrieving items %s through %s from the REST API" %(str(offset), str(offset + self.limit)))
            params['offset'] = offset
            params['limit'] = self.limit

            items_url = self.construct_url(command = 'items', params = params)
            self.logger.debug("Items Solr call: %s" %(items_url))
            items = self.rest_call(url = items_url)

            if len(items) == 0:
                break

            all_items = all_items + items
            offset = offset + self.limit

        return all_items

    def find_items_by_metadata_field(self, metadata_entry=None, expand=[]):
        if metadata_entry is None:
            return

        params = {}
        expandValue = ''

        if len(expand) > 0:
            expandValue = ','.join(expand)
            params['expand'] = expandValue
            self.logger.debug("Added expand list to parameters: %s " %(expandValue))

        headers = self.headers
        headers['Content-Type'] = 'application/json'

        items_metadata_url = self.construct_url(command = f"items/find-by-metadata-field", params = params)
        self.logger.info(items_metadata_url)
        self.logger.info(metadata_entry)
        items = self.rest_call(type = 'POST', url = items_metadata_url, headers = headers, data = metadata_entry)
        return items

    def get_item(self, item_id=None):
        if item_id is None:
            return

        item_url = self.construct_url(command = f"items/{item_id}")
        item = self.rest_call(url = item_url)
        return item