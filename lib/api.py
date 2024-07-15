"""Class for interacting with a DSpace 7+ REST API"""

import logging
import requests


class DSpaceRestApi():
    """Class for interacting with a DSpace 7+ REST API"""

    def __init__(self, rest_server=None):
        # Ensure URL of rest_server has trailing slash
        url = rest_server['url']
        if url[len(url)-1] != '/':
            self.api_url = url + '/'
        else:
            self.api_url = url

        self.logger = logging.getLogger('dspace-reports')
        self.logger.debug("Connecting to DSpace REST API:  %s.", self.api_url)

        self.username = rest_server['username']
        self.password = rest_server['password']

        # Construct token URL
        self.token_url = self.api_url + "security/csrf"

        # Construct login URL
        self.login_url = self.api_url + "authn/login"

        # Create session
        self.session = requests.Session()

        # Get CSRF token
        self.token = None
        self.get_token()

        self.limit = 100

        self.auth_headers = {}
        self.request_headers = {'Content-type': 'application/json'}
        self.cookies = {}

        # Authenticate using parameters in set here
        self.authenticated = self.authenticate()
        if self.authenticated is False:
            return None

        # Test connection to REST API
        self.test_connection()

    def get_token(self):
        """Get CSRF token"""

        token_response = self.session.get(self.token_url)
        if 'DSPACE-XSRF-TOKEN' in token_response.headers:
            self.token = token_response.headers['DSPACE-XSRF-TOKEN']
            self.session.headers.update({'X-XSRF-Token': self.token})
            self.session.cookies.update({'X-XSRF-Token': self.token})
            self.logger.debug("Updating CSRF token to: %s", self.token)
        else:
            self.logger.info('No DSPACE-XSRF-TOKEN in the API response.')


    def authenticate(self):
        """Authenticate a REST API user"""

        self.logger.info("Authenticating connection to REST API")

        # Create data dictionary with credentials
        data = {'user':self.username, 'password':self.password}

        # Attempt to log in to REST API
        login_response = self.session.post(self.login_url, headers=self.auth_headers, data=data)
        self.logger.info("Calling REST API: %s", login_response.url)

        if login_response.status_code == 200:
            self.logger.info("Successfully authenticated: %s", login_response.status_code)
            self.logger.info(login_response.cookies)

            if 'Authorization' in login_response.headers:
                self.session.headers.update({'Authorization': login_response.headers.get('Authorization')})

            return True

        self.logger.info("REST API authentication failed: %s", login_response.status_code)
        self.logger.info(login_response.text)
        return False

    def test_connection(self):
        """"Test REST API connection"""

        if self.authenticated is False:
            self.logger.error("Must authenticate before connecting to the REST API.")
            return False

        self.logger.info("Testing REST API connection: %s.", self.api_url.strip("/"))
        response = self.session.get(self.api_url.strip("/"), headers=self.request_headers)
        if response.status_code == 200:
            self.logger.info("REST API connection successful.")
            return True

        self.logger.error("REST API connection NOT successful.")
        return False

    def construct_url(self, command, params=None):
        """Construct API URL"""

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

        final_url = self.api_url + command + parameters
        return final_url

    def rest_call(self, call_type='GET', url='', headers=None, data=None):
        """Make call to REST API"""

        if headers is None:
            headers = self.session.headers

        if data is None:
            data = {}

        self.logger.debug("Calling REST API with URL: %s", url)

        if call_type == 'POST':
            response = self.session.post(url, headers=headers, cookies=self.cookies, data=data)
        else:
            response = self.session.get(url, headers=headers, cookies=self.cookies)

        self.logger.debug(response.status_code)
        self.logger.debug(response.text)
        response_json = response.json()
        return response_json

    def get_site(self):
        """Get site information"""

        sites_url = self.api_url + 'core/sites'
        sites_response = self.rest_call(url=sites_url)
        if '_embedded' in sites_response:
            if 'sites' in sites_response['_embedded']:
                site = sites_response['_embedded']['sites'][0]
                return site

        return None


    def get_communities(self):
        """Get all communities"""

        communities_url = self.construct_url(command = 'core/communities')
        communities = self.rest_call(url = communities_url)
        return communities

    def get_top_level_communities(self):
        """Get top level communities"""

        top_communities_url = self.construct_url(command = 'core/communities/search/top')
        top_communities = self.rest_call(url = top_communities_url)
        return top_communities

    def get_community(self, community_uuid=None):
        """Get an individual community"""

        if community_uuid is None:
            return None
        community_url = self.construct_url(command = f"core/communities/{community_uuid}")
        community = self.rest_call(url = community_url)
        return community

    def get_collection_items(self, collection_uuid=None):
        """Get items of a collection"""

        if collection_uuid is None:
            return None
        items_url = self.construct_url(command = f"core/collections/{collection_uuid}/items")
        items = self.rest_call(url = items_url)
        return items

    def get_items(self, expand=None):
        """Get all items in the repository"""

        if expand is None:
            expand = []

        offset = 0
        params = {}
        expand_value = ''
        all_items = []

        if len(expand) > 0:
            expand_value = ','.join(expand)
            params['expand'] = expand_value
            self.logger.debug("Added expand list to parameters: %s ", expand_value)

        while True:
            self.logger.debug("Retrieving items %s through %s from the REST API", offset,
                              offset + self.limit)
            params['offset'] = offset
            params['limit'] = self.limit

            items_url = self.construct_url(command = 'items', params = params)
            self.logger.debug("Items Solr call: %s", items_url)
            items = self.rest_call(url = items_url)

            if len(items) == 0:
                break

            all_items = all_items + items
            offset = offset + self.limit

        return all_items

    def find_items_by_metadata_field(self, metadata_entry=None, expand=None):
        """Find an item by any metadata field(s)"""

        if metadata_entry is None:
            return None

        if expand is None:
            expand = []

        params = {}
        expand_value = ''

        if len(expand) > 0:
            expand_value = ','.join(expand)
            params['expand'] = expand_value
            self.logger.debug("Added expand list to parameters: %s ", expand_value)

        items_metadata_url = self.construct_url(command = "items/find-by-metadata-field",
                                                params = params)
        self.logger.info(items_metadata_url)
        self.logger.info(metadata_entry)
        items = self.rest_call(call_type = 'POST', url = items_metadata_url,
                               headers = self.request_headers, data = metadata_entry)
        return items

    def get_item(self, item_id=None):
        """Get an individual item"""

        if item_id is None:
            return None

        item_url = self.construct_url(command = f"items/{item_id}")
        item = self.rest_call(url = item_url)
        return item

    def update_token(self, req):
        """Update CSRF token"""

        if not self.session:
            self.logger.debug('Session state not found, setting...')
            self.session = requests.Session()
        if 'DSPACE-XSRF-TOKEN' in req.headers:
            t = req.headers['DSPACE-XSRF-TOKEN']
            self.logger.debug('Updating XSRF token to %s', t)

            # Update headers and cookies
            self.session.headers.update({'X-XSRF-Token': t})
            self.session.cookies.update({'X-XSRF-Token': t})
