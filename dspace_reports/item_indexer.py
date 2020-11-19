import sys

from lib.oai import DSpaceOai
from lib.database import Database
from dspace_reports.indexer import Indexer


class ItemIndexer(Indexer):
    def __init__(self, config):
        super().__init__(config)

        # Create OAI-PMH server object
        self.oai = DSpaceOai(oai_server=config['oai_server'])
        if self.oai is None:
            self.logger.error("Unable to create Indexer due to earlier failures creating a connection to OAI-PMH feed.")
            sys.exit(1)

        # Set time periods to only month and year as all can cause Solr to crash
        self.time_periods = ['month', 'year']

    def index(self):
        # Get list of identifiers from OAI-PMH feed
        records = self.oai.get_records()
        self.logger.debug("Found %s records in OAI-PMH feed." %(str(len(records))))

        # Iterate over OAI-PMH records and call REST API for addiional metadata
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                for record in records:
                    self.logger.debug("Calling REST API for record: %s" %(record))

                    metadata_entry = '{"key":"dc.identifier.uri", "value":"%s"}' %(record)
                    items = self.rest.find_items_by_metadata_field(metadata_entry=metadata_entry, expand=['parentCollection'])
                    if len(items) == 1:
                        item = items[0]
                        item_id = item['uuid']
                        item_name = item['name']

                        # Attempt to get collection name
                        item_collection_name = "Unknown"
                        if 'parentCollection' in item:
                            item_collection = item['parentCollection']
                            item_collection_name = item_collection['name']
                        
                            self.logger.info("item collection: %s " %(item_collection_name))

                        # If name is null then use "Untitled"
                        if item_name is not None:
                            # If item name is longer than 255 characters then shorten it to fit in database field
                            if len(item_name) > 255:
                                item_name = item_name[0:251] + "..."
                        else:
                            item_name = "Untitled"

                        # Create handle URL for item
                        item_url = self.base_url + item['handle']

                        self.logger.debug(cursor.mogrify("INSERT INTO item_stats (collection_name, item_id, item_name, item_url) VALUES (%s, %s, %s, %s)", (item_collection_name, item_id, item_name, item_url)))

                        db.commit()
                    else:
                        self.logger.error('Unable to find item in REST API.')
        
        for time_period in self.time_periods:
            self.logger.info("Indexing Solr views for time period: %s ", time_period)
            self.index_views(time_period=time_period)

            self.logger.info("Indexing Solr downloads for time period: %s ", time_period)
            self.index_downloads(time_period=time_period)

    def index_views(self, time_period='all'):
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
        solr_query_params = {
            "q": "type:2",
            "fq": "isBot:false AND statistics_type:view",
            "facet": "true",
            "facet.field": "id",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "id",
            "stats.calcdistinct": "true",
            "shards": shards,
            "rows": 0,
            "wt": "json"
        }

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get date range for Solr query if time period is specified
        date_range = []
        if time_period != 'all':
            date_range = self.get_date_range(time_period)
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"
            else:
                self.logger.error("Error creating date range.")

        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Solr total item views query: %s", response.url)
        
        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["id"][
                "countDistinct"
            ]
            self.logger.info("Solr total item views count: %s", str(results_totalNumFacets))
        except TypeError:
            self.logger.info("No item views to index")
            return

        # divide results into "pages" (cast to int to effectively round down)
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    self.logger.info("Indexing item views (page %s of %s" %(str(results_current_page + 1), str(results_num_pages + 1)))

                    solr_query_params = {
                        "q": "type:2",
                        "fq": "isBot:false AND statistics_type:view",
                        "facet": "true",
                        "facet.field": "id",
                        "facet.mincount": 1,
                        "facet.limit": results_per_page,
                        "facet.offset": results_current_page * results_per_page,
                        "shards": shards,
                        "rows": 0,
                        "wt": "json",
                        "json.nl": "map",  # return facets as a dict instead of a flat list
                    }

                    # Create base Solr url
                    solr_url = self.solr_server + "/statistics/select"

                    # Add date range for Solr query if time period is specified
                    if len(date_range) == 2:
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    # Make call to Solr for views statistics
                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.debug("Calling page %s of Solr item views: %s" %(str(results_current_page +1), response.url))

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    self.logger.info('items in this batch : %s', str(len(views["id"].items())))

                    # Loop through list of item views
                    for item_id, item_views in views["id"].items():
                        if self.validate_uuid4(item_id):
                            self.logger.info("Updating item views stats with %s views from item: %s" %(str(item_views), item_id))
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (str(item_views), item_id)))
                                cursor.execute("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (str(item_views), item_id))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_year = %s WHERE item_id = %s", (str(item_views), item_id)))
                                cursor.execute("UPDATE item_stats SET views_last_year = %s WHERE item_id = %s", (str(item_views), item_id))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (str(item_views), item_id)))
                                cursor.execute("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (str(item_views), item_id))
                        elif isinstance(item_id, str) and '-unmigrated' in item_id:
                            self.logger.debug("Item ID was not migrated to UUID: %s (views: %s)" %(item_id, str(item_views)))
                        else:
                            self.logger.debug("Item ID is not valid: %s (views: %s)" %(item_id), str(item_views))
                            
                    # Commit changes
                    db.commit()

                    results_current_page += 1


    def index_downloads(self, time_period='all'):
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
        solr_query_params = {
            "q": "type:0",
            "fq": "isBot:false AND statistics_type:view AND bundleName:ORIGINAL",
            "facet": "true",
            "facet.field": "owningItem",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "owningItem",
            "stats.calcdistinct": "true",
            "shards": shards,
            "rows": 0,
            "wt": "json"
        }

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get date range for Solr query if time period is specified
        date_range = []
        if time_period != 'all':
            date_range = self.get_date_range(time_period)
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"
            else:
                self.logger.error("Error creating date range.")

        # Make call to Solr for download statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Solr total item downloads query: %s", response.url)

        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
            self.logger.info("Solr total item downloads count: %s", str(results_totalNumFacets))
        except TypeError:
            self.logger.info("No item downloads to index.")
            return

        # Calculate pagination
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    self.logger.info("Indexing item downloads (page %s of %s" %(str(results_current_page + 1), str(results_num_pages + 1)))

                    solr_query_params = {
                        "q": "type:0",
                        "fq": "isBot:false AND statistics_type:view AND bundleName:ORIGINAL",
                        "facet": "true",
                        "facet.field": "owningItem",
                        "facet.mincount": 1,
                        "facet.limit": results_per_page,
                        "facet.offset": results_current_page * results_per_page,
                        "shards": shards,
                        "rows": 0,
                        "wt": "json",
                        "json.nl": "map"
                    }

                    # Create base Solr url
                    solr_url = self.solr_server + "/statistics/select"

                    # Add date range for Solr query if time period is specified
                    if len(date_range) == 2:
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    # Make call to Solr for downloads statistics
                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.debug("Calling page %s of Solr item downloads: %s" %(str(results_current_page +1), response.url))

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    self.logger.info('Items in this batch : %s', str(len(downloads["owningItem"].items())))

                    # Loop through list of item downloads
                    for item_id, item_downloads in downloads["owningItem"].items():
                        if self.validate_uuid4(item_id):
                            self.logger.info("Updating item downloads stats with %s downloads from item: %s" %(str(item_downloads), item_id))
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (str(item_downloads), item_id)))
                                cursor.execute("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (str(item_downloads), item_id))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_last_year = %s WHERE item_id = %s", (str(item_downloads), item_id)))
                                cursor.execute("UPDATE item_stats SET downloads_last_year = %s WHERE item_id = %s", (str(item_downloads), item_id))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (str(item_downloads), item_id)))
                                cursor.execute("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (str(item_downloads), item_id))
                        elif isinstance(item_id, str) and '-unmigrated' in item_id:
                            self.logger.debug("Item ID was not migrated to UUID: %s (downloads: %s)" %(item_id, str(item_downloads)))
                        else:
                            self.logger.debug("Item ID is not valid: %s (downloads: %s)" %(item_id, str(item_downloads)))

                    # Commit changes
                    db.commit()

                    results_current_page += 1
