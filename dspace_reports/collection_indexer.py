"""Class for indexing collections"""

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
                self.logger.debug("Collection name is longer than 255 characters. " +
                                  "It will be shortened to that length.")
                collection_name = collection_name[0:251] + "..."

            # Insert the collection into the database
            with Database(self.config['database']) as db:
                with db.cursor() as cursor:
                    self.logger.debug(cursor.mogrify("INSERT INTO collection_stats (collection_id, collection_name, collection_url, parent_community_name) VALUES (%s, %s, %s, %s)", (collection_uuid, collection_name, collection_url, parent_community_name)))
                    cursor.execute("INSERT INTO collection_stats (collection_id, collection_name, collection_url, parent_community_name) VALUES (%s, %s, %s, %s)", (collection_uuid, collection_name, collection_url, parent_community_name))
                    db.commit()

            for time_period in self.time_periods:
                self.logger.info("Indexing items for collection: %s (%s)", collection_name,
                                 collection_uuid)
                self.index_collection_items(collection_uuid=collection_uuid, time_period=time_period)

                # Index all views and downloads of collections
                self.logger.info("Updating views statistics for collection during time period: %s",
                             time_period)
                self.index_collection_views(collection_uuid=collection_uuid, time_period=time_period)

                self.logger.info("Updating downloads statistics for collection during time period: %s",
                                time_period)
                self.index_collection_downloads(collection_uuid=collection_uuid, time_period=time_period)

    def index_collection_items(self, collection_uuid=None, time_period=None):
        """Index the collection items"""

        if collection_uuid is None or time_period is None:
            return

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
        else:
            self.logger.error("Error creating date range.")

        # Add collection UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND location.coll:" + collection_uuid

        # Make call to Solr for items statistics
        response = self.solr.query_search(params=solr_query_params)
        self.logger.info("Calling Solr items in collection: %s", response.url)

        results_total_items = 0
        try:
            # Get total number of items
            results_total_items = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_total_items))
        except TypeError:
            self.logger.info("No collection items to index.")
            return

        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_last_month = %s WHERE collection_id = %s", (results_total_items, collection_uuid)))
                    cursor.execute("UPDATE collection_stats SET items_last_month = %s WHERE collection_id = %s", (results_total_items, collection_uuid))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_academic_year = %s WHERE collection_id = %s", (results_total_items, collection_uuid)))
                    cursor.execute("UPDATE collection_stats SET items_academic_year = %s WHERE collection_id = %s", (results_total_items, collection_uuid))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_total = %s WHERE collection_id = %s", (results_total_items, collection_uuid)))
                    cursor.execute("UPDATE collection_stats SET items_total = %s WHERE collection_id = %s", (results_total_items, collection_uuid))

                # Commit changes
                db.commit()

    def index_collection_views(self, collection_uuid=None, time_period=None):
        """Index the collection views"""

        if collection_uuid is None or time_period is None:
            return

        # Get date range for Solr query if time period is specified
        solr_date_string = ""
        date_range = []
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s",
                                date_range[0], date_range[1])
            if date_range[0] is not None and date_range[1] is not None:
                date_start = date_range[0]
                date_end = date_range[1]
                solr_date_string = f"time:[{date_start} TO {date_end}]"

        print(f"Indexing collection views for collection: {collection_uuid} " +
              "during time: {solr_date_string}.")

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Update database
        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                # Solr params
                solr_query_params = {
                    "q": f"owningColl:({collection_uuid})",
                    "fq": f"type:2 AND -isBot:true AND statistics_type:view AND {solr_date_string}",
                    "fl": "owningColl",
                    "facet": "true",
                    "facet.field": "owningColl",
                    "facet.mincount": 1,
                    "shards": shards,
                    "rows": 0,
                    "wt": "json",
                    "json.nl": "map",  # return facets as a dict instead of a flat list
                }

                response = self.solr.query_statistics(params=solr_query_params)
                self.logger.info("Solr collection views query: %s", response.url)

                # Solr returns facets as a dict of dicts (see json.nl parameter)
                views = response.json()["facet_counts"]["facet_fields"]

                self.logger.debug("Found %s results in Solr", len(views))

                # Iterate over the facetField dict and get the UUIDs and views
                for collection_id, collection_views in views["owningColl"].items():
                    if collection_id == collection_uuid:
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_month = %s WHERE collection_id = %s", (collection_views, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET views_last_month = %s WHERE collection_id = %s", (collection_views, collection_uuid))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_academic_year = %s WHERE collection_id = %s", (collection_views, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET views_academic_year = %s WHERE collection_id = %s", (collection_views, collection_uuid))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_total = %s WHERE collection_id = %s", (collection_views, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET views_total = %s WHERE collection_id = %s", (collection_views, collection_uuid))
                    
                        # Commit changes to database
                        db.commit()
                    else:
                        self.logger.warning("Solr query returned results for a different collection UUID: %s", collection_id)

    def index_collection_downloads(self, collection_uuid=None, time_period=None):
        """Index the collection downloads"""

        if collection_uuid is None or time_period is None:
            return

        # Get date range for Solr query if time period is specified
        solr_date_string = ""
        date_range = []
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s",
                                date_range[0], date_range[1])
            if date_range[0] is not None and date_range[1] is not None:
                date_start = date_range[0]
                date_end = date_range[1]
                solr_date_string = f"time:[{date_start} TO {date_end}]"

        print(f"Indexing collection downloads for collection: {collection_uuid} " +
              "during time: {solr_date_string}.")

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Update database
        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                # Solr params
                solr_query_params = {
                    "q": f"owningColl:({collection_uuid})",
                    "fq": f"type:0 AND -isBot:true AND statistics_type:view AND bundleName:ORIGINAL AND {solr_date_string}",
                    "fl": "owningColl",
                    "facet": "true",
                    "facet.field": "owningColl",
                    "facet.mincount": 1,
                    "shards": shards,
                    "rows": 0,
                    "wt": "json",
                    "json.nl": "map",  # return facets as a dict instead of a flat list
                }

                response = self.solr.query_statistics(params=solr_query_params)
                self.logger.info("Solr collection downloads query: %s", response.url)

                # Solr returns facets as a dict of dicts (see json.nl parameter)
                downloads = response.json()["facet_counts"]["facet_fields"]

                self.logger.debug("Found %s results in Solr", len(downloads))

                # Iterate over the facetField dict and get the UUIDs and views
                for collection_id, collection_downloads in downloads["owningColl"].items():
                    if collection_id == collection_uuid:
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_month = %s WHERE collection_id = %s", (collection_downloads, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET downloads_last_month = %s WHERE collection_id = %s", (collection_downloads, collection_uuid))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_academic_year = %s WHERE collection_id = %s", (collection_downloads, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET downloads_academic_year = %s WHERE collection_id = %s", (collection_downloads, collection_uuid))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_total = %s WHERE collection_id = %s", (collection_downloads, collection_uuid)))
                            cursor.execute("UPDATE collection_stats SET downloads_total = %s WHERE collection_id = %s", (collection_downloads, collection_uuid))
                    
                        # Commit changes to database
                        db.commit()
                    else:
                        self.logger.warning("Solr query returned results for a different collection UUID: %s", collection_id)
