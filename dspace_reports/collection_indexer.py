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
        community_id = community['id']
        community_name = community['name']
        self.logger.info("Loading collections of community %s (%s)" %(community_name, community_id))

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
                        cursor.execute("INSERT INTO collection_stats (parent_community_name, collection_id, collection_name, collection_url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (community_name, collection_id, collection_name, collection_url))
                        db.commit()

                for time_period in self.time_periods:
                    self.logger.info("Indexing items for collection: %s (%s)" %(collection_id, collection_name))
                    self.index_collection_items(collection_id=collection_id, time_period=time_period)

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
        
    def index_collection_items(self, collection_id=None, time_period=None):
        if collection_id is None or time_period is None:
            return

        # Create base Solr URL
        solr_url = self.solr_server + "/search/select"
        self.logger.debug("tdl solr_url: %s" %(solr_url))
        
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)
        
        # Default Solr params
        solr_query_params = {
            "q": "search.resourcetype:2",
            "wt": "json"
        }

        # Get date range for Solr query if time period is specified
        date_range = []
        if time_period != 'all':
            self.logger.debug("Creating date range for time period: %s" %(time_period))
            date_range = self.get_date_range(time_period)
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params["fq"] = f"dc.date.accessioned_dt:[{date_start} TO {date_end}]"
            else:
                self.logger.error("Error creating date range.")

        # Add community UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND location.coll:" + collection_id

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total items in community: %s", response.url)
        
        results_totalItems = 0
        try:
            # Get total number of items
            results_totalItems = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_totalItems))
        except TypeError:
            self.logger.info("No item to index, returning.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_last_month = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_last_month = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_last_year = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_last_year = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_total = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_total = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))

                # Commit changes
                db.commit()

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
        
        # Total collection views count
        total_collection_views = 0

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

        # Iterate over Solr results
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
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

            # Make call to Solr for views statistics
            response = self.solr.call(url=solr_url, params=solr_query_params)
            self.logger.debug("Calling page %s of Solr item views in collection: %s" %(str(results_current_page +1), response.url))

            # Solr returns facets as a dict of dicts (see json.nl parameter)
            views = response.json()["facet_counts"]["facet_fields"]
            self.logger.info('Items in this batch : %s', str(len(views["id"].items())))

            # Loop through list of item views
            for item_id, item_views in views["id"].items():
                self.logger.info("Updating collection views stats with %s views from item: %s" %(str(item_views), item_id))
                total_collection_views = total_collection_views + item_views

            results_current_page += 1
        
        self.logger.info("Total collection views: %s" %(str(total_collection_views)))
        
        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_month = views_last_month + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id)))
                    cursor.execute("UPDATE collection_stats SET views_last_month = views_last_month + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_year = views_last_year + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id)))
                    cursor.execute("UPDATE collection_stats SET views_last_year = views_last_year + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_total = views_total + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id)))
                    cursor.execute("UPDATE collection_stats SET views_total = views_total + %i WHERE collection_id = '%s'" %(total_collection_views, collection_id))

            # Commit changes
            db.commit()

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
        
        # Total collection downloads count
        total_collection_downloads = 0

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

        # Iterate over Solr results
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
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

            # Make call to Solr for views statistics
            response = self.solr.call(url=solr_url, params=solr_query_params)
            self.logger.debug("Calling page %s of Solr item downloads in collection: %s" %(str(results_current_page +1), response.url))

            # Solr returns facets as a dict of dicts (see json.nl parameter)
            downloads = response.json()["facet_counts"]["facet_fields"]
            
            # Loop through list of item downloads
            for item_id, item_downloads in downloads["owningItem"].items():
                self.logger.info("Updating collection downloads stats with %s views from item: %s" %(str(item_downloads), item_id))
                total_community_downloads = total_collection_downloads + item_downloads

            results_current_page += 1

        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_month = downloads_last_month + %i WHERE collection_id = '%s'" %(total_collection_downloads, collection_id)))
                    cursor.execute("UPDATE collection_stats SET downloads_last_month = downloads_last_month + %i WHERE collection_id = '%s'" %(total_collection_downloads, collection_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_year = downloads_last_year + %i WHERE collection_id = '%s'" %(total_collection_downloads, collection_id)))
                    cursor.execute("UPDATE collection_stats SET downloads_last_year = downloads_last_year + %i WHERE collecction_id = '%s'" %(total_collection_downloads, collection_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_total = downloads_total + %i WHERE collection_id = '%s'" %(total_collection_downloads, collection_id)))
                    cursor.execute("UPDATE collection_stats SET downloads_total = downloads_total + %i WHERE collection_id = '%s'" %(total_collection_downloads, collection_id))

            # Commit changes
            db.commit()
