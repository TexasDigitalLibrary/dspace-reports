from lib.database import Database
from dspace_reports.indexer import Indexer


class CollectionIndexer(Indexer):
    def index(self):
        # Get site hierarchy
        hierarchy = self.rest.get_hierarchy()

        # Traverse hierarchy
        self.logger.info(hierarchy)

        self.logger.info("Loading DSpace collections...")
        self.index_collections()

    def index_collections(self):
        # List of collections
        collections = []

        # Get site hierarchy
        hierarchy = self.rest.get_hierarchy()

        if 'community' in hierarchy:
            communities = hierarchy['community']
            self.logger.info("Repository has %s top-level communities.", str(len(communities)))

            for community in communities:
                self.load_collections_recursive(collections, community)
        else:
            self.logger.info("Repository has no communities.")
                
    def load_collections_recursive(self, collections, community):
        self.logger.info("Loading collections of community %s (%s)" %(community['name'], community['id']))

        if 'collection' in community:
            collections = community['collection']
            self.logger.info("Community has %s collections.", str(len(collections)))
            for collection in collections:
                collection_id = collection['id']
                collection_name = collection['name']
                collection_handle = collection['handle']
                collection_url = self.base_url + collection_handle
                self.logger.info("Loading collection: %s (%s)..." %(collection_name, collection_id))

                # Insert the collection into the database
                with Database(self.config['statistics_db']) as db:
                    with db.cursor() as cursor:
                        cursor.execute("INSERT INTO collection_stats (collection_id, collection_name, collection_url) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (collection_id, collection_name, collection_url))
                        db.commit()

                for time_period in self.time_periods:
                    self.logger.info("Updating views statistics for collection %s during time period: %s" %(collection_id, time_period))
                    self.index_collection_views(collection_id=collection_id, time_period=time_period)

                    self.logger.info("Updating downloads statistics for collection %s during time period: %s" %(collection_id, time_period))
                    self.index_collection_downloads(collection_id=collection_id, time_period=time_period)
        else:
            self.logger.info("There are no collections in this community.")

        if 'community' in community:
            sub_communities = community['community']
            for sub_community in sub_communities:
                self.load_collections_recursive(collections, sub_community)
        else:
            self.logger.info("There are no subcommunities in this community.")
        

    def index_collection_views(self, collection_id=None, time_period=None):
        if collection_id is None or time_period is None:
            return
        
        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Default Solr params
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

        # Add collection UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND owningColl:" + collection_id

        # Get date range for Solr query if time period is specified
        date_range = str()
        date_range = self.get_date_range(time_period)
        if len(date_range) > 0:
            self.logger.debug("Searching date range: %s", date_range)
            solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range
        
        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total item views in collection: %s", response.url)
        
        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["id"][
                "countDistinct"
            ]
            self.logger.info("Solr views - total items: %s", str(results_totalNumFacets))
        except TypeError:
            self.logger.info("No item views to index, returning.")
            return

        # Calculate pagination
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    self.logger.info("Indexing item views (page %s of %s)" %(str(results_current_page + 1), str(results_num_pages + 1)))

                    # Construct Solr params for current results
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
                        "json.nl": "map"
                    }

                    # Add collection UUID to query parameter
                    solr_query_params['q'] = solr_query_params['q'] + " AND owningColl:" + collection_id

                    # Get date range for Solr query if time period is specified
                    if len(date_range) > 0:
                        self.logger.debug("Searching date range: %s", date_range)
                        solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

                    # Make call to Solr for views statistics
                    response = self.solr.call(url=solr_url, params=solr_query_params)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    self.logger.info('Items in this batch : %s', str(len(views["id"].items())))

                    # Loop through list of item views
                    for item_id, item_views in views["id"].items():
                        self.logger.info("Updating community views stats with %s views from item: %s" %(str(item_views), item_id))
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_month = %i WHERE collection_id = '%s'" %(item_views, collection_id)))
                            cursor.execute("UPDATE collection_stats SET views_last_month = %i WHERE collection_id = '%s'" %(item_views, collection_id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_year = %i WHERE collection_id = '%s'" %(item_views, collection_id)))
                            cursor.execute("UPDATE collection_stats SET views_last_year = %i WHERE collection_id = '%s'" %(item_views, collection_id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_total = %i WHERE collection_id = '%s'" %(item_views, collection_id)))
                            cursor.execute("UPDATE collection_stats SET views_total = %i WHERE collection_id = '%s'" %(item_views, collection_id))

                    # Commit changes
                    db.commit()

                    results_current_page += 1

    def index_collection_downloads(self, collection_id=None, time_period=None):
        if collection_id is None or time_period is None:
            return
        
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Default Solr params
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

        # Add collection UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND owningColl:" + collection_id

        # Get date range for Solr query if time period is specified
        date_range = str()
        date_range = self.get_date_range(time_period)
        if len(date_range) > 0:
            self.logger.debug("Searching date range: %s", date_range)
            solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range
        
        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total item downloads in collection: %s", response.url)

        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item downloads to index, returning.")
            return

        # Calculate pagination
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    self.logger.info("Indexing item downloads (page %s of %s)" %(str(results_current_page + 1), str(results_num_pages + 1)))

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

                    # Add collection UUID to query parameter
                    solr_query_params['q'] = solr_query_params['q'] + " AND owningColl:" + collection_id

                    # Get date range for Solr query if time period is specified
                    if len(date_range) > 0:
                        self.logger.debug("Searching date range: %s", date_range)
                        solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

                    # Make call to Solr for views statistics
                    response = self.solr.call(url=solr_url, params=solr_query_params)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    
                    # Loop through list of item downloads
                    for item_id, item_downloads in downloads["owningItem"].items():
                        self.logger.info("Updating collection downloads stats with %s views from item: %s" %(str(item_downloads), item_id))
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_month = downloads_last_month + %i WHERE collection_id = '%s'" %(item_downloads, collection_id)))
                            cursor.execute("UPDATE collection_stats SET downloads_last_month = downloads_last_month + %i WHERE collection_id = '%s'" %(item_downloads, collection_id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_year = downloads_last_year + %i WHERE collection_id = '%s'" %(item_downloads, collection_id)))
                            cursor.execute("UPDATE collection_stats SET downloads_last_year = downloads_last_year + %i WHERE collection_id = '%s'" %(item_downloads, collection_id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_total = downloads_total + %i WHERE collection_id = '%s'" %(item_downloads, collection_id)))
                            cursor.execute("UPDATE collection_stats SET downloads_total = downloads_total + %i WHERE collection_id = '%s'" %(item_downloads, collection_id))

                    # Commit changes
                    db.commit()

                    results_current_page += 1
