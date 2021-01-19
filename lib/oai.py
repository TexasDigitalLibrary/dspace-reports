import re
import requests
import logging
from time import sleep
from xml.dom import pulldom

import xml.etree.ElementTree as ET


class DSpaceOai(object):
    ns = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'dc': 'http://purl.org/dc/elements/1.1/'
    }

    def __init__(self, oai_server=None):
        # Ensure solr_server has trailing slash
        if oai_server[len(oai_server)-1] != '/':
            self.oai_server = oai_server + '/'
        else:
            self.oai_server = oai_server

        # Add 'request' to path
        self.oai_server = self.oai_server + 'request'

        self.limit = 100
        self.sleepTime = 1
        self.headers = {'User-Agent': 'OAIHarvester/2.0', 'Accept': 'text/html',
               'Accept-Encoding': 'compress, deflate'}

        self.logger = logging.getLogger('dspace-reports')
        
        # Test connection to OAI-PMH feed
        self.test_connection()

    def test_connection(self):
        identify_url = self.construct_url(verb='Identify')
        self.logger.info("Testing OAI-PMH feed connection: %s.", identify_url)
        response = self.call(url = identify_url)

        if response.status_code == 200:
            self.logger.info("OAI_PMH feed connection successful.")
            return True
        else:
            self.logger.error("OAI-PMH feed connection NOT successful.")
            return False

    def construct_url(self, verb, params={}):
        parameters = ''
        for key, value in params.items():
            parameters += '&' + key + '=' + str(value)

        new_url = self.oai_server + '?verb=' + verb + parameters
        return new_url

    def call(self, url=None, params={}):
        if url is None:
            return

        response = requests.get(url, params=params)
        return response

    def pause(self, wait_time):
        self.logger.info("Pausing harvest process for %s second(s)." %(str(wait_time)))
        sleep(wait_time)

    def get_records(self):
        offset = 0
        all_records = []
        params = {
            'metadataPrefix': 'oai_dc'
        }

        while True:
            self.logger.debug("Retrieving records %s through %s from the OAI-PMH feed." %(str(offset), str(offset + self.limit)))
            records_url = self.construct_url(verb = 'ListRecords', params = params)
            self.logger.debug("Records OAI-PMH call: %s" %(records_url))

            records_response = self.call(url = records_url)
            records_root = ET.fromstring(records_response.text)
            
            list_records = records_root.find('.//oai:ListRecords', self.ns)
            if list_records:
                records = list_records.findall('.//oai:record', self.ns)
                for record in records:
                    metadata = record.find('.//oai:metadata', self.ns)
                    if metadata:
                        identifier_node = metadata.find('.//dc:identifier', self.ns)
                        if identifier_node is not None and identifier_node.text is not None:
                            self.logger.info("Looking at record identifier: %s : %s" %(identifier_node.tag, identifier_node.text))
                            self.logger.info(identifier_node)
                            if 'handle' in identifier_node.text:
                                all_records.append(identifier_node.text)
                            else:
                                self.logger.debug("Identifier is not a handle URL: %s" %(identifier_node.text))

            # Check for resumptionToken
            token_match = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', records_response.text)
            if not token_match:
                break

            token = token_match.group(1)
            self.logger.debug("resumptionToken: %s" %(token))
            params['resumptionToken'] = token

            # Remove metadataPrefix from params
            if 'metadataPrefix' in params:
                params.pop('metadataPrefix')

            offset = offset + self.limit

            if self.sleepTime:
                self.pause(self.sleepTime)

        self.logger.debug("Harvested %s records from OAI feed." %(str(len(all_records))))
        return all_records