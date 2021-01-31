import math
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
        self.time_periods = ['month', 'year', 'all']

    def index(self):
        # Get list of identifiers from OAI-PMH feed
        records = self.oai.get_records()
        total_records = len(records)
        self.logger.info("Found %s records in OAI-PMH feed." %(str(total_records)))

        # Keep a count of records that cannot be found by their metadata
        count_records = 0
        count_missing_records = 0

        # Iterate over OAI-PMH records and call REST API for addiional metadata
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                for record in records:
                    count_records = count_records + 1
                    self.logger.info("(%s/%s) - Calling REST API for record: %s" %(str(count_records), str(total_records), record))

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

                        if len(item_collection_name) > 255:
                            self.logger.debug("Collection name is longer than 255 characters. It will be shortened to that length.")
                            item_collection_name = item_collection_name[0:251] + "..."
                        
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

                        self.logger.debug(cursor.mogrify("INSERT INTO item_stats (collection_name, item_id, item_name, item_url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (item_collection_name, item_id, item_name, item_url)))
                        cursor.execute("INSERT INTO item_stats (collection_name, item_id, item_name, item_url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (item_collection_name, item_id, item_name, item_url))
                        db.commit()
                    else:
                        count_missing_records += 1
                        self.logger.error("Unable to find item in REST API: %s" %(record))
        
        self.logger.info("Total records in OAI-PMH feed: %s" %(str(len(records))))
        if count_missing_records > 0 and total_records > 0:
            self.logger.info("Total records missing in OAI-PMH feed: %s (%.0f%%)" %(str(count_missing_records), (100 * count_missing_records/total_records)))

        for time_period in self.time_periods:
            self.logger.info("Indexing Solr views for time period: %s ", time_period)
            self.index_item_views(time_period=time_period)

            self.logger.info("Indexing Solr downloads for time period: %s ", time_period)
            self.index_item_downloads(time_period=time_period)

    def index_item_views(self, time_period='all'):
        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
        solr_query_params = {
            "q": f"type:2 AND id:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view",
            "fl": "id",
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
            "wt": "json",
        }

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

        # Make call to Solr for total views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Solr total item views query: %s", response.url)
        
        try:
            # get total number of distinct facets (countDistinct)
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["id"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item views to index.")
            return

        # divide results into "pages" and round up to next integer
        results_per_page = 100
        results_num_pages = math.ceil(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:

                while results_current_page <= results_num_pages:
                    print(
                        f"Indexing item views (page {results_current_page + 1} of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:2 AND id:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view",
                        "fl": "id",
                        "facet": "true",
                        "facet.field": "id",
                        "facet.mincount": 1,
                        "facet.limit": results_per_page,
                        "facet.offset": results_current_page * results_per_page,
                        "shards": shards,
                        "rows": 0,
                        "wt": "json",
                        "json.nl": "map",
                    }

                    if len(date_range) == 2:
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr item views query: %s", response.url)
 
                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # iterate over the facetField dict and get the ids and views
                    for id, item_views in views["id"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_views, id)))
                            cursor.execute("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_views, id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_academic_year = %s WHERE item_id = %s", (item_views, id)))
                            cursor.execute("UPDATE item_stats SET views_academic_year = %s WHERE item_id = %s", (item_views, id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_views, id)))
                            cursor.execute("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_views, id))
            
                    # Commit changes to database
                    db.commit()

                    results_current_page += 1

    def index_item_downloads(self, time_period='all'):
        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
        solr_query_params = {
            "q": f"type:0 AND owningItem:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
            "fl": "owningItem",
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
            "wt": "json",
        }

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
            # get total number of distinct facets (countDistinct)
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item downloads to index.")
            return

        results_per_page = 100
        results_num_pages = math.ceil(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:

                while results_current_page <= results_num_pages:
                    # "pages" are zero based, but one based is more human readable
                    print(
                        f"Indexing item downloads (page {results_current_page + 1} of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:0 AND owningItem:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
                        "fl": "owningItem",
                        "facet": "true",
                        "facet.field": "owningItem",
                        "facet.mincount": 1,
                        "facet.limit": results_per_page,
                        "facet.offset": results_current_page * results_per_page,
                        "shards": shards,
                        "rows": 0,
                        "wt": "json",
                        "json.nl": "map",
                    }

                    if len(date_range) == 2:
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr item downloads query: %s", response.url)
 
                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # iterate over the facetField dict and get the ids and views
                    for id, item_downloads in downloads["owningItem"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_downloads, id)))
                            cursor.execute("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_downloads, id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_academic_year = %s WHERE item_id = %s", (item_downloads, id)))
                            cursor.execute("UPDATE item_stats SET downloads_academic_year = %s WHERE item_id = %s", (item_downloads, id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_downloads, id)))
                            cursor.execute("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_downloads, id))
            
                    # Commit changes to database
                    db.commit()

                    results_current_page += 1
