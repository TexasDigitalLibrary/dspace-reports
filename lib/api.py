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

    def rest_call(self, call_type='GET', url='', params=None, data=None, headers=None):
        """Make call to REST API"""

        if params is None:
            params = {}

        if data is None:
            data = {}

        if headers is None:
            headers = self.session.headers

        self.logger.debug("Calling REST API with URL: %s", url)

        if call_type == 'GET':
            response = self.session.get(url, params=params, headers=headers, cookies=self.cookies)
        else:
            response = self.session.post(url, data=data, params=params,
                                         cookies=self.cookies, headers=headers)

        if response.status_code == 200:
            return response.json()

        # Log errors
        if response.status_code >= 400 and response.status_code <= 500:
            self.logger.error("Error while making rest call, (HTTP code: %s) %s",
                                response.status_code, response.text)

        return None

    def get_site(self):
        """Get site information"""

        sites_url = self.api_url + 'core/sites'
        sites_response = self.rest_call(url=sites_url)
        if '_embedded' in sites_response:
            if 'sites' in sites_response['_embedded']:
                site = sites_response['_embedded']['sites'][0]
                return site

        return None

    def get_communities(self, sort=None):
        """Get all communities"""

        params = {}
        if sort is not None:
            params['sort'] = sort

        communities = []
        page = 0
        params['page'] = page
        size = 20
        params['size'] = size

        communities_url = self.construct_url(command = 'core/communities')
        total_communities = 0
        total_pages = 0

        while True:
            self.logger.info("Loading page %s of communities...", str(page))

            communities_response = self.rest_call(url = communities_url, params = params)
            if communities_response is not None and '_embedded' in communities_response:
                # Get ccommunities from this page of results
                if 'communities' in communities_response['_embedded']:
                    self.logger.info(communities_response['_embedded']['communities'])
                    for community_json in communities_response['_embedded']['communities']:
                        communities.append(community_json)

                # Check API response for amount of total communities and pages
                if 'page' in communities_response:
                    page_info = communities_response['page']
                    if 'totalElements' in page_info:
                        total_communities = page_info['totalElements']
                    if 'totalPages' in page_info:
                        total_pages = page_info['totalPages']

                page += 1
                if total_pages > 0 and page == total_pages:
                    break

                params['page'] = page
            else:
                break

        # Sanity check to make sure all pages were retrieved
        if len(communities) != total_communities:
            self.logger.error("There was a problem retrieving communities from the API.")
            self.logger.error("Communities retrieved: %s. Total communities reported by API: %s",
                              str(len(communities)), str(total_communities))
        else:
            self.logger.info("Retrieved %s communities from the REST API.", str(len(communities)))

        return communities

    def get_top_level_communities(self):
        """Get top level communities"""

        top_communities = []
        top_communities_url = self.construct_url(command = 'core/communities/search/top')
        top_communities_response = self.rest_call(url = top_communities_url)
        if top_communities_response is not None and '_embedded' in top_communities_response:
            if 'communities' in top_communities_response['_embedded']:
                top_communities = top_communities_response['_embedded']['communities']

        return top_communities

    def get_community(self, community_uuid=None):
        """Get an individual community"""

        if community_uuid is None:
            return None

        community = None
        community_url = self.construct_url(command = f"core/communities/{community_uuid}")
        community_response = self.rest_call(url = community_url)
        if community_response is not None:
            community = community_response[0]

        return community

    def get_community_parent_community(self, community_uuid=None):
        """Get parent community of a given community"""

        if community_uuid is None:
            return None

        parent_community_url = self.construct_url(
            command = f"core/communities/{community_uuid}/parentCommunity")

        return self.rest_call(url = parent_community_url)

    def get_collections(self, sort=None):
        """Get all collections"""

        params = {}
        if sort is not None:
            params['sort'] = sort

        collections = []
        page = 0
        params['page'] = page
        size = 20
        params['size'] = size

        collections_url = self.construct_url(command = 'core/collections')
        total_collections = 0
        total_pages = 0

        while True:
            self.logger.info("Loading page %s of collections...", str(page))

            collections_response = self.rest_call(url = collections_url, params = params)
            if collections_response is not None and '_embedded' in collections_response:
                # Get collections from this page of results
                if 'collections' in collections_response['_embedded']:
                    self.logger.info(collections_response['_embedded']['collections'])
                    for collection_json in collections_response['_embedded']['collections']:
                        collections.append(collection_json)

                # Check API response for amount of total collections and pages
                if 'page' in collections_response:
                    page_info = collections_response['page']
                    if 'totalElements' in page_info:
                        total_collections = page_info['totalElements']
                    if 'totalPages' in page_info:
                        total_pages = page_info['totalPages']

                page += 1
                if total_pages > 0 and page == total_pages:
                    break

                params['page'] = page
            else:
                break

        # Sanity check to make sure all pages were retrieved
        if len(collections) != total_collections:
            self.logger.error("There was a problem retrieving collections from the API.")
            self.logger.error("Collections retrieved: %s. Total collections reported by API: %s",
                              str(len(collections)), str(total_collections))
        else:
            self.logger.info("Retrieved %s collection(s) from the REST API.", str(len(collections)))

        return collections

    def get_collection_parent_community(self, collection_uuid=None):
        """Get parent community of a given collection"""

        if collection_uuid is None:
            return None

        parent_community_url = self.construct_url(
            command = f"core/collections/{collection_uuid}/parentCommunity")
        return self.rest_call(url = parent_community_url)

    def get_collection_items(self, collection_uuid=None):
        """Get items of a collection"""

        if collection_uuid is None:
            return None
        items_url = self.construct_url(command = f"core/collections/{collection_uuid}/items")
        items = self.rest_call(url = items_url)
        return items

    def get_items(self, sort=None):
        """Get all items"""

        params = {}
        if sort is not None:
            params['sort'] = sort

        items = []
        page = 0
        params['page'] = page
        size = 100
        params['size'] = size

        items_url = self.construct_url(command = 'core/items')
        total_items = 0
        total_pages = 0

        while True:
            self.logger.info("Loading page %s of items...", str(page))

            items_response = self.rest_call(url = items_url, params = params)
            if items_response is not None and '_embedded' in items_response:
                # Get items from this page of results
                if 'items' in items_response['_embedded']:
                    self.logger.info(items_response['_embedded']['items'])
                    for item_json in items_response['_embedded']['items']:
                        items.append(item_json)

                # Check API response for amount of total items and pages
                if 'page' in items_response:
                    page_info = items_response['page']
                    if 'totalElements' in page_info:
                        total_items = page_info['totalElements']
                    if 'totalPages' in page_info:
                        total_pages = page_info['totalPages']

                page += 1
                if total_pages > 0 and page == total_pages:
                    break

                params['page'] = page
            else:
                break

        # Sanity check to make sure all pages were retrieved
        if len(items) != total_items:
            self.logger.error("There was a problem retrieving items from the API.")
            self.logger.error("Items retrieved: %s. Total items reported by API: %s",
                              str(len(items)), str(total_items))
        else:
            self.logger.info("Retrieved %s items(s) from the REST API.", str(len(items)))

        return items

    def find_items_by_metadata_field(self, metadata_entry=None, expand=None):
        """Find an item by any metadata field(s)"""

        if metadata_entry is None:
            return None

        if expand is None:
            expand = []

        params = {}
        expand_value = ''

        items = []

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

    def get_item(self, item_uuid=None):
        """Get an individual item"""

        if item_uuid is None:
            return None

        item_url = self.construct_url(command = f"core/items/{item_uuid}")
        item = self.rest_call(url = item_url)
        return item

    def get_item_owning_collection(self, item_uuid=None):
        """Get owning collection of a given item"""

        if item_uuid is None:
            return None

        item_owning_collection_url = self.construct_url(
            command = f"core/items/{item_uuid}/owningCollection")
        item_owning_collection = self.rest_call(url = item_owning_collection_url)
        return item_owning_collection

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
