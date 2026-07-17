"""Class for indexing items"""

import math
from time import sleep

from lib.database import Database
from dspace_reports.indexer import Indexer


class ItemIndexer(Indexer):
    """Class for indexing items"""

    def __init__(self, config, logger):
        super().__init__(config, logger)

        # Set time periods to only month and year as all can cause Solr to crash
        self.time_periods = ['month', 'year', 'all']

        # Set crawl delay from config
        self.crawl_delay = config['crawl_delay']

        # Get author field(s) from configuration
        author_fields_string = config['item_author_fields']
        self.author_fields = author_fields_string.split(',')

    def index(self):
        # Get list of identifiers from REST API
        items = self.rest.get_items()
        total_items = len(items)
        self.logger.info("Found %s records in REST API.", str(total_items))

        # Keep a count of records that cannot be found by their metadata
        count_items = 0

        # Iterate over records and call REST API for additional metadata
        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                for item in items:
                    count_items += 1

                    # Get item metadata
                    item_uuid = item['uuid']
                    item_name = item['name']

                    self.logger.info("Item : %s (%s)", item_name, item_uuid)

                    # Attempt to get collection name
                    item_owning_collection_name = "Unknown"
                    item_owning_collection = self.rest.get_item_owning_collection(
                        item_uuid=item_uuid)
                    if item_owning_collection is not None:
                        item_owning_collection_name = item_owning_collection['name']

                    if len(item_owning_collection_name) > 255:
                        self.logger.debug("Collection name is longer than 255 characters. " +
                                            "It will be shortened to that length.")
                        item_owning_collection_name = item_owning_collection_name[0:251] + "..."

                    self.logger.info("Item owning collection: %s ", item_owning_collection_name)

                    # If name is None then use "Untitled"
                    if item_name is not None:
                        # If item name is longer than 255 characters then shorten it
                        # to fit in database field
                        if len(item_name) > 255:
                            item_name = item_name[0:251] + "..."
                    else:
                        item_name = "Untitled"

                    # Create Handle URL for item
                    item_url = ''
                    if 'handle' in item and item['handle'] is not None:
                        item_url = self.base_url + item['handle']
                    else:
                        self.logger.warning("Item is missing a handle.")
                        if 'metadata' in item:
                            metadata = item['metadata']
                            if 'dc.identifier.uri' in metadata:
                                self.logger.debug("The dc.identifier.uri key is in the metadata.")
                                item_url_metadata = metadata['dc.identifier.uri'][0]
                                if 'value' in item_url_metadata:
                                    item_url = item_url_metadata['value']
                            else:
                                self.logger.debug("The dc.identifier.uri key is not in"
                                                  + " the metadata")

                    # Fall back to default string if no Handle URL
                    if len(item_url) == 0:
                        self.logger.warning("The item URL is empty.")
                        item_url = "Unable to find handle/URL"

                    # Add author(s)
                    item_authors = ''
                    if 'metadata' in item:
                        metadata = item['metadata']
                        for author_field in self.author_fields:
                            if author_field in metadata:
                                self.logger.debug("The %s author field is in the metadata.",
                                                  str(author_field))
                                author_field_authors = metadata[author_field]
                                for author_field_author in author_field_authors:
                                    if 'value' in author_field_author:
                                        item_authors += author_field_author['value'] + "; "

                    item_authors = item_authors.removesuffix("; ")
                    if len(item_authors) > 100:
                        item_authors = item_authors[:97] + "..."

                    self.logger.debug("The item authors: %s", str(item_authors))

                    # Add date issued
                    item_date_issued = ''
                    if 'metadata' in item:
                        metadata = item['metadata']
                        if 'dc.date.issued' in metadata:
                            self.logger.debug("The dc.date.issued key is in the metadata.")
                            item_url_metadata = metadata['dc.date.issued'][0]
                            if 'value' in item_url_metadata:
                                item_date_issued = item_url_metadata['value']
                        else:
                            self.logger.debug("The dc.date.issued key is not in the metadata")


                    self.logger.debug("Item URL: %s", item_url)

                    cursor.execute("INSERT INTO item_stats (collection_name, item_id, item_name, item_authors, item_date_issued, item_url) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (item_owning_collection_name, item_uuid, item_name, item_authors, item_date_issued, item_url))
                    db.commit()

        for time_period in self.time_periods:
            self.logger.info("Indexing Solr views for time period: %s ", time_period)
            self.index_item_views(time_period=time_period)

            self.logger.info("Indexing Solr downloads for time period: %s ", time_period)
            self.index_item_downloads(time_period=time_period)

    def index_item_views(self, time_period='all'):
        """Index the item views"""

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
                solr_query_params['q'] = (solr_query_params['q'] + " AND " +
                                          f"time:[{date_start} TO {date_end}]")
        else:
            self.logger.error("Error creating date range.")

        # Make call to Solr for total views statistics
        response = self.solr.query_statistics(params=solr_query_params)
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

        with Database(self.config['database']) as db:
            with db.cursor() as cursor:

                while results_current_page <= results_num_pages:
                    print(
                        f"Indexing item views (page {results_current_page + 1} " +
                        f"of {results_num_pages + 1})"
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
                            solr_query_params['q'] = (solr_query_params['q'] + " AND " +
                                                      f"time:[{date_start} TO {date_end}]")

                    response = self.solr.query_statistics(params=solr_query_params)
                    self.logger.info("Solr item views query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and views
                    for item_uuid, item_views in views["id"].items():
                        if len(item_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_views, item_uuid)))
                                cursor.execute("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_views, item_uuid))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_academic_year = %s WHERE item_id = %s", (item_views, item_uuid)))
                                cursor.execute("UPDATE item_stats SET views_academic_year = %s WHERE item_id = %s", (item_views, item_uuid))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_views, item_uuid)))
                                cursor.execute("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_views, item_uuid))
                        else:
                            self.logger.warning("Item ID value is not a UUID: %s",
                                                item_uuid)

                    # Commit changes to database
                    db.commit()

                    if self.crawl_delay:
                        sleep(self.crawl_delay)

                    results_current_page += 1

    def index_item_downloads(self, time_period='all'):
        """Index the item downloads"""

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
                solr_query_params['q'] = (solr_query_params['q'] + " AND " +
                                          f"time:[{date_start} TO {date_end}]")
        else:
            self.logger.error("Error creating date range.")

        # Make call to Solr for download statistics
        response = self.solr.query_statistics(params=solr_query_params)
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

        with Database(self.config['database']) as db:
            with db.cursor() as cursor:

                while results_current_page <= results_num_pages:
                    # "pages" are zero based, but one based is more human readable
                    print(
                        f"Indexing item downloads (page {results_current_page + 1} " +
                        f"of {results_num_pages + 1})"
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
                        self.logger.info("Searching date range: %s - %s",
                                         date_range[0], date_range[1])
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = (solr_query_params['q'] + " AND " +
                                                      f"time:[{date_start} TO {date_end}]")

                    response = self.solr.query_statistics(params=solr_query_params)
                    self.logger.info("Solr item downloads query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and downloads
                    for item_uuid, item_downloads in downloads["owningItem"].items():
                        if len(item_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_downloads, item_uuid)))
                                cursor.execute("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_downloads, item_uuid))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_academic_year = %s WHERE item_id = %s", (item_downloads, item_uuid)))
                                cursor.execute("UPDATE item_stats SET downloads_academic_year = %s WHERE item_id = %s", (item_downloads, item_uuid))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_downloads, item_uuid)))
                                cursor.execute("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_downloads, item_uuid))
                        else:
                            self.logger.warning("Item ID value is not a UUID: %s",
                                                item_uuid)

                    # Commit changes to database
                    db.commit()

                    if self.crawl_delay:
                        sleep(self.crawl_delay)

                    results_current_page += 1
