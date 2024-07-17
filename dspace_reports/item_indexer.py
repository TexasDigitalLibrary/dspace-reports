"""Class for indexing items"""

import math
import sys

from time import sleep

from lib.oai import DSpaceOai
from lib.database import Database
from dspace_reports.indexer import Indexer


class ItemIndexer(Indexer):
    """Class for indexing items"""

    def __init__(self, config, logger):
        super().__init__(config, logger)

        # Set time periods to only month and year as all can cause Solr to crash
        self.time_periods = ['month', 'year', 'all']

        # Set crawl delay from config
        self.delay = config['delay']

    def index(self):
        # Get list of identifiers from REST API
        items = self.rest.get_items()
        total_items = len(items)
        self.logger.info("Found %s records in REST API.", str(total_items))

        # Keep a count of records that cannot be found by their metadata
        count_items = 0

        # Iterate over records and call REST API for additional metadata
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                for item in items:
                    count_items += 1

                    # Get item metadata
                    item_uuid = item['uuid']
                    item_name = item['name']

                    # Attempt to get collection name
                    item_owning_collection_name = "Unknown"
                    item_owning_collection = self.rest.get_item_owning_collection(
                        item_uuid=item_uuid)
                    if item_owning_collection is not None:
                        self.logger.info(item_owning_collection)
                        item_owning_collection_name = item_owning_collection['name']

                    if len(item_owning_collection_name) > 255:
                        self.logger.debug("Collection name is longer than 255 characters. " +
                                            "It will be shortened to that length.")
                        item_owning_collection_name = item_owning_collection_name[0:251] + "..."

                        self.logger.info("Item owning collection: %s ", item_owning_collection_name)

                    # If name is null then use "Untitled"
                    if item_name is not None:
                        # If item name is longer than 255 characters then shorten it
                        # to fit in database field
                        if len(item_name) > 255:
                            item_name = item_name[0:251] + "..."
                    else:
                        item_name = "Untitled"

                    # Create handle URL for item
                    item_url = self.base_url + item['handle']

                    self.logger.debug(cursor.mogrify(f"INSERT INTO item_stats (collection_name, item_id, item_name, item_url) VALUES ('{item_owning_collection_name}', '{item_uuid}, '{item_name}', '{item_url}') ON CONFLICT DO NOTHING"))
                    cursor.execute(f"INSERT INTO item_stats (collection_name, item_id, item_name, item_url) VALUES ('{item_owning_collection_name}', '{item_uuid}', '{item_name}', '{item_url}') ON CONFLICT DO NOTHING")
                    db.commit()

        for time_period in self.time_periods:
            self.logger.info("Indexing Solr views for time period: %s ", time_period)
            self.index_item_views(time_period=time_period)

            self.logger.info("Indexing Solr downloads for time period: %s ", time_period)
            self.index_item_downloads(time_period=time_period)

    def index_item_views(self, time_period='all'):
        """Index the item views"""

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

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
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s", date_range[0], date_range[1])
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
            results_total_num_facets = response.json()["stats"]["stats_fields"]["id"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item views to index.")
            return

        # divide results into "pages" and round up to next integer
        results_per_page = 100
        results_num_pages = math.ceil(results_total_num_facets / results_per_page)
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
                        self.logger.info("Searching date range: %s - %s",
                                         date_range[0], date_range[1])
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr item views query: %s", response.url)
 
                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and views
                    for item_uuid, item_views in views["id"].items():
                        if len(item_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET views_last_month = {item_views} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET views_last_month = {item_views} WHERE item_id = '{item_uuid}'")
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET views_academic_year = {item_views} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET views_academic_year = {item_views} WHERE item_id = '{item_uuid}'")
                            else:
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET views_total = {item_views} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET views_total = {item_views} WHERE item_id = '{item_uuid}'")
                        else:
                            self.logger.warning("Item ID value is not a UUID: %s",
                                                item_uuid)

                    # Commit changes to database
                    db.commit()

                    if self.delay:
                        sleep(self.delay)

                    results_current_page += 1

    def index_item_downloads(self, time_period='all'):
        """Index the item downloads"""

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

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
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s", date_range[0], date_range[1])
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
            results_total_num_facets = response.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item downloads to index.")
            return

        results_per_page = 100
        results_num_pages = math.ceil(results_total_num_facets / results_per_page)
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
                        self.logger.info("Searching date range: %s - %s", date_range[0], date_range[1])
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr item downloads query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and downloads
                    for item_uuid, item_downloads in downloads["owningItem"].items():
                        if len(item_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET downloads_last_month = {item_downloads} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET downloads_last_month = {item_downloads} WHERE item_id = '{item_uuid}'")
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET downloads_academic_year = {item_downloads} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET downloads_academic_year = {item_downloads} WHERE item_id = '{item_uuid}'")
                            else:
                                self.logger.debug(cursor.mogrify(f"UPDATE item_stats SET downloads_total = {item_downloads} WHERE item_id = '{item_uuid}'"))
                                cursor.execute(f"UPDATE item_stats SET downloads_total = {item_downloads} WHERE item_id = '{item_uuid}'")
                        else:
                            self.logger.warning("Item ID value is not a UUID: %s",
                                                item_uuid)

                    # Commit changes to database
                    db.commit()

                    if self.delay:
                        sleep(self.delay)

                    results_current_page += 1
