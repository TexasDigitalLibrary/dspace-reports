"""Class for indexing collections"""

import math

from lib.database import Database
from dspace_reports.indexer import Indexer


class CollectionIndexer(Indexer):
    """Class for indexing collections"""

    def index(self):
        """Index function"""

        self.logger.info("Loading DSpace collections...")
        self.index_collections()

    def index_collections(self):
        """Index the collections in the repository"""

        # Get a list of all collections from the REST API
        collections = self.rest.get_collections()
        for collection in collections:
            collection_uuid = collection['uuid']
            collection_name = collection['name']
            self.logger.info("Loading collection: %s (%s)...", collection_name, collection_uuid)

            # Get collection metadata, including parent community name
            collection_handle = collection['handle']
            collection_url = self.base_url + collection_handle

            parent_community_name = "Unknown"
            parent_community = self.rest.get_collection_parent_community(
                collection_uuid=collection_uuid)
            if 'name' in parent_community:
                parent_community_name = parent_community['name']

            if len(collection_name) > 255:
                self.logger.debug("Collection name is longer than 255 characters. It will be shortened to that length.")
                collection_name = collection_name[0:251] + "..."

            # Insert the collection into the database
            with Database(self.config['statistics_db']) as db:
                with db.cursor() as cursor:
                    cursor.execute(f"INSERT INTO collection_stats (parent_community_name, collection_id, collection_name, collection_url) VALUES ('{parent_community_name}', '{collection_uuid}', '{collection_name}', '{collection_url}') ON CONFLICT DO NOTHING")
                    db.commit()

            for time_period in self.time_periods:
                self.logger.info("Indexing items for collection: %s (%s)", collection_name,
                                 collection_uuid)
                self.index_collection_items(collection_uuid=collection_uuid, time_period=time_period)

        # Index all views and downloads of collections
        for time_period in self.time_periods:
            self.logger.info("Updating views statistics for collections during time period: %s",
                             time_period)
            self.index_collection_views(time_period=time_period)

            self.logger.info("Updating downloads statistics for collection during time period: %s",
                             time_period)
            self.index_collection_downloads(time_period=time_period)

    def index_collection_items(self, collection_uuid=None, time_period=None):
        """Index the collection items"""

        if collection_uuid is None or time_period is None:
            return

        # Create base Solr URL
        solr_url = self.solr_server + "/search/select"
        self.logger.debug("TDL Solr_URL: %s", solr_url)

        # Default Solr params
        solr_query_params = {
            "q": "search.resourcetype:Item",
            "start": "0",
            "rows": "0",
            "wt": "json"
        }

        # Get date range for Solr query if time period is specified
        date_range = []
        self.logger.debug("Creating date range for time period: %s", time_period)
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s", date_range[0], date_range[1])
            if date_range[0] is not None and date_range[1] is not None:
                date_start = date_range[0]
                date_end = date_range[1]
                solr_query_params["fq"] = f"dc.date.accessioned_dt:[{date_start} TO {date_end}]"
        else:
            self.logger.error("Error creating date range.")

        # Add collection UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND location.coll:" + collection_uuid

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total items in community: %s", response.url)

        results_total_items = 0
        try:
            # Get total number of items
            results_total_items = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_total_items))
        except TypeError:
            self.logger.info("No collection items to index, returning.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET items_last_month = {results_total_items} WHERE collection_id = '{collection_uuid}'"))
                    cursor.execute(f"UPDATE collection_stats SET items_last_month = {results_total_items} WHERE collection_id = '{collection_uuid}'")
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET items_academic_year = {results_total_items} WHERE collection_id = '{collection_uuid}'"))
                    cursor.execute(f"UPDATE collection_stats SET items_academic_year = {results_total_items} WHERE collection_id = '{collection_uuid}'")
                else:
                    self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET items_total = {results_total_items} WHERE collection_id = '{collection_uuid}'"))
                    cursor.execute(f"UPDATE collection_stats SET items_total = {results_total_items} WHERE collection_id = '{collection_uuid}'")

                # Commit changes
                db.commit()

    def index_collection_views(self, time_period=None):
        """Index the collection views"""

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Default Solr params
        solr_query_params = {
            "q": f"type:2 AND owningColl:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
            "fl": "owningColl",
            "facet": "true",
            "facet.field": "owningColl",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "owningColl",
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

        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total collection views in collections: %s", response.url)

        try:
            # Get total number of distinct facets (countDistinct)
            results_total_num_facets = response.json()["stats"]["stats_fields"]["owningColl"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No collection views to index.")
            return

        # Divide results into "pages" and round up to next integer
        results_per_page = 100
        results_num_pages = math.ceil(results_total_num_facets / results_per_page)
        results_current_page = 0

        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    print(
                        f"Indexing collection views (page {results_current_page + 1} of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:2 AND owningColl:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
                        "fl": "owningColl",
                        "facet": "true",
                        "facet.field": "owningColl",
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
                    self.logger.info("Solr collection views query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the ids and views
                    for collection_uuid, collection_views in views["owningColl"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET views_last_month = {collection_views} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET views_last_month = {collection_views} WHERE collection_id = '{collection_uuid}'")
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET views_academic_year = {collection_views} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET views_academic_year = {collection_views} WHERE collection_id = '{collection_uuid}'")
                        else:
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET views_total = {collection_views} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET views_total = {collection_views} WHERE collection_id = '{collection_uuid}'")

                    # Commit changes to database
                    db.commit()

                    results_current_page += 1


    def index_collection_downloads(self, time_period=None):
        """Index the collection downloads"""

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Default Solr params
        solr_query_params = {
            "q": f"type:0 AND owningColl:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
            "fl": "owningColl",
            "facet": "true",
            "facet.field": "owningColl",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "owningColl",
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

        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total collection downloads in collections: %s", response.url)

        try:
            # get total number of distinct facets (countDistinct)
            results_total_num_facets = response.json()["stats"]["stats_fields"]["owningColl"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No collection downloads to index.")
            return

        results_per_page = 100
        results_num_pages = math.ceil(results_total_num_facets / results_per_page)
        results_current_page = 0

        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    # "pages" are zero based, but one based is more human readable
                    print(
                        f"Indexing collection downloads (page {results_current_page + 1} of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:0 AND owningColl:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
                        "fl": "owningColl",
                        "facet": "true",
                        "facet.field": "owningColl",
                        "facet.mincount": 1,
                        "facet.limit": results_per_page,
                        "facet.offset": results_current_page * results_per_page,
                        "shards": shards,
                        "rows": 0,
                        "wt": "json",
                        "json.nl": "map",
                    }

                    if len(date_range) == 2:
                        self.logger.info("Searching date range: %s - %s", date_range[0],
                                         date_range[1])
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr collection downloads query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the ids and views
                    for collection_uuid, collection_downloads in downloads["owningColl"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET downloads_last_month = {collection_downloads} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET downloads_last_month = {collection_downloads} WHERE collection_id = '{collection_uuid}'")
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET downloads_academic_year = {collection_downloads} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET downloads_academic_year = {collection_downloads} WHERE collection_id = '{collection_uuid}")
                        else:
                            self.logger.debug(cursor.mogrify(f"UPDATE collection_stats SET downloads_total = {collection_downloads} WHERE collection_id = '{collection_uuid}'"))
                            cursor.execute(f"UPDATE collection_stats SET downloads_total = {collection_downloads} WHERE collection_id = '{collection_uuid}'")

                    # Commit changes to database
                    db.commit()

                    results_current_page += 1
