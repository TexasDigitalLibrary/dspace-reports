import math

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

        for time_period in self.time_periods:
            self.logger.info("Updating views statistics for collections during time period: %s" %(time_period))
            self.index_collection_views(time_period=time_period)

            self.logger.info("Updating downloads statistics for collection during time period: %s" %(time_period))
            self.index_collection_downloads(time_period=time_period)

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

                if len(collection_name) > 255:
                    self.logger.debug("Collection name is longer than 255 characters. It will be shortened to that length.")
                    collection_name = collection_name[0:251] + "..."

                # Insert the collection into the database
                with Database(self.config['statistics_db']) as db:
                    with db.cursor() as cursor:
                        cursor.execute("INSERT INTO collection_stats (parent_community_name, collection_id, collection_name, collection_url) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (community_name, collection_id, collection_name, collection_url))
                        db.commit()

                for time_period in self.time_periods:
                    self.logger.info("Indexing items for collection: %s (%s)" %(collection_id, collection_name))
                    self.index_collection_items(collection_id=collection_id, time_period=time_period)
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
        
        # Default Solr params
        solr_query_params = {
            "q": "search.resourcetype:2",
            "start": "0",
            "rows": "0",
            "wt": "json"
        }

        # Get date range for Solr query if time period is specified
        date_range = []
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
            self.logger.info("No collection items to index, returning.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_last_month = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_last_month = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_academic_year = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_academic_year = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE collection_stats SET items_total = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id)))
                    cursor.execute("UPDATE collection_stats SET items_total = %i WHERE collection_id = '%s'" %(results_totalItems, collection_id))

                # Commit changes
                db.commit()

    def index_collection_views(self, time_period=None):
        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Default Solr params
        solr_query_params = {
            "q": f"type:2 AND owningColl:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view",
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
            self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
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
            # get total number of distinct facets (countDistinct)
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningColl"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No collection views to index.")
            return

        # divide results into "pages" and round up to next integer
        results_per_page = 100
        results_num_pages = math.ceil(results_totalNumFacets / results_per_page)
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
                        "fq": "-isBot:true AND statistics_type:view",
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
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr collection views query: %s", response.url)
 
                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # iterate over the facetField dict and get the ids and views
                    for id, collection_views in views["owningColl"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_last_month = %s WHERE collection_id = %s", (collection_views, id)))
                            cursor.execute("UPDATE collection_stats SET views_last_month = %s WHERE collection_id = %s", (collection_views, id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_academic_year = %s WHERE collection_id = %s", (collection_views, id)))
                            cursor.execute("UPDATE collection_stats SET views_academic_year = %s WHERE collection_id = %s", (collection_views, id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET views_total = %s WHERE collection_id = %s", (collection_views, id)))
                            cursor.execute("UPDATE collection_stats SET views_total = %s WHERE collection_id = %s", (collection_views, id))
            
                    # Commit changes to database
                    db.commit()

                    results_current_page += 1


    def index_collection_downloads(self, time_period=None):
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

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
            self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
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
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningColl"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No collection downloads to index.")
            return

        results_per_page = 100
        results_num_pages = math.ceil(results_totalNumFacets / results_per_page)
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
                        self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                        if date_range[0] is not None and date_range[1] is not None:
                            date_start = date_range[0]
                            date_end = date_range[1]
                            solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"
                            
                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr collection downloads query: %s", response.url)
 
                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # iterate over the facetField dict and get the ids and views
                    for id, collection_downloads in downloads["owningColl"].items():
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_last_month = %s WHERE collection_id = %s", (collection_downloads, id)))
                            cursor.execute("UPDATE collection_stats SET downloads_last_month = %s WHERE collection_id = %s", (collection_downloads, id))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_academic_year = %s WHERE collection_id = %s", (collection_downloads, id)))
                            cursor.execute("UPDATE collection_stats SET downloads_academic_year = %s WHERE collection_id = %s", (collection_downloads, id))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE collection_stats SET downloads_total = %s WHERE collection_id = %s", (collection_downloads, id)))
                            cursor.execute("UPDATE collection_stats SET downloads_total = %s WHERE collection_id = %s", (collection_downloads, id))
            
                    # Commit changes to database
                    db.commit()

                    results_current_page += 1