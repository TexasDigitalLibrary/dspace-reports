from lib.database import Database
from dspace_reports.indexer import Indexer


class CommunityIndexer(Indexer):
    def index(self):
        # Get site hierarchy
        hierarchy = self.rest.get_hierarchy()

        # Traverse hierarchy
        self.logger.info(hierarchy)

        self.logger.info("Loading DSpace communities...")
        self.index_communities()

    def index_communities(self):
        # List of communities
        communities = []

        # Get site hierarchy
        hierarchy = self.rest.get_hierarchy()

        if 'community' in hierarchy:
            communities = hierarchy['community']
            self.logger.info("Repository has %s top-level communities.", str(len(communities)))

            for community in communities:
                self.logger.debug("Loading top-level community: %s (%s)" %(community['name'], community['id']))
                self.load_communities_recursive(communities, community)
        else:
            self.logger.info("Repository has no communities.")
                
    def load_communities_recursive(self, communities, community, parent_community_name=""):
        # Extract metadata
        community_id = community['id']
        community_name = community['name']
        community_handle = community['handle']
        community_url = self.base_url + community_handle
        self.logger.info("Loading community: %s (%s)..." %(community_name, community_id))

         # Insert the community into the database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.debug(cursor.mogrify("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_id, community_name, community_url, parent_community_name)))
                cursor.execute("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_id, community_name, community_url, parent_community_name))
                db.commit()

        # Index views and downloads for the current community
        for time_period in self.time_periods:
            self.logger.info("Indexing items for community: %s (%s)" %(community_id, community_name))
            self.index_community_items(community_id=community_id, time_period=time_period)

            self.logger.info("Indexing views for community: %s (%s)" %(community_id, community_name))
            self.index_community_views(community_id=community_id, time_period=time_period)

            self.logger.info("Indexing downloads starting with community: %s (%s)" %(community_id, community_name))
            self.index_community_downloads(community_id=community_id, time_period=time_period)

        # Load sub communities
        if 'community' in community:
            sub_communities = community['community']
            for sub_community in sub_communities:
                self.logger.info("Loading subcommunity: %s (%s)" %(sub_community['name'], sub_community['id']))
                self.load_communities_recursive(communities=communities, community=sub_community, parent_community_name=community_name)
        else:
            self.logger.info("There are no subcommunities in this community.")
        
    def index_community_items(self, community_id=None, time_period=None):
        if community_id is None or time_period is None:
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
        solr_query_params['q'] = solr_query_params['q'] + " AND location.comm:" + community_id

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total items in community: %s", response.url)
        
        results_totalItems = 0
        try:
            # Get total number of items
            results_totalItems = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_totalItems))
        except TypeError:
            self.logger.info("No item to index.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_last_month = %i WHERE community_id = '%s'" %(results_totalItems, community_id)))
                    cursor.execute("UPDATE community_stats SET items_last_month = %i WHERE community_id = '%s'" %(results_totalItems, community_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_last_year = %i WHERE community_id = '%s'" %(results_totalItems, community_id)))
                    cursor.execute("UPDATE community_stats SET items_last_year = %i WHERE community_id = '%s'" %(results_totalItems, community_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_total = %i WHERE community_id = '%s'" %(results_totalItems, community_id)))
                    cursor.execute("UPDATE community_stats SET items_total = %i WHERE community_id = '%s'" %(results_totalItems, community_id))

                # Commit changes
                db.commit()

    def index_community_views(self, community_id=None, time_period=None):
        if community_id is None or time_period is None:
            return
        
        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Default Solr params
        solr_query_params = {
            "q": "type:2",
            "fq": "isBot:false AND statistics_type:view",
            "shards": shards,
            "rows": 0,
            "wt": "json"
        }

        # Add community UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND owningComm:" + community_id

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
        self.logger.info("Calling Solr total item views in community: %s", response.url)
        
        try:
            # Get total number of items
            results_num_found = response.json()["response"]["numFound"]
            self.logger.info("Solr views - total items: %s", str(results_num_found))
        except TypeError:
            self.logger.info("No item views to index.")
            return

        self.logger.info("Total community views: %s" %(str(results_num_found)))
        
        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_last_month = views_last_month + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET views_last_month = views_last_month + %i WHERE community_id = '%s'" %(results_num_found, community_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_last_year = views_last_year + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET views_last_year = views_last_year + %i WHERE community_id = '%s'" %(results_num_found, community_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_total = views_total + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET views_total = views_total + %i WHERE community_id = '%s'" %(results_num_found, community_id))

            # Commit changes
            db.commit()
        
    def index_community_downloads(self, community_id=None, time_period=None):
        if community_id is None or time_period is None:
            return
        
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Default Solr params
        solr_query_params = {
            "q": "type:0",
            "fq": "isBot:false AND statistics_type:view AND bundleName:ORIGINAL",
            "shards": shards,
            "rows": 0,
            "wt": "json"
        }

        # Add community UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND owningComm:" + community_id

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
    
        # Make call to Solr for downloads statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total item downloads in community: %s", response.url)

        try:
            # Get total number of items
            results_num_found = response.json()["response"]["numFound"]
            self.logger.info("Solr downloads - total items: %s", str(results_num_found))
        except TypeError:
            self.logger.info("No item downloads to index.")
            return

        self.logger.info("Total community downloads: %s" %(str(results_num_found)))
        
        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_last_month = downloads_last_month + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET downloads_last_month = downloads_last_month + %i WHERE community_id = '%s'" %(results_num_found, community_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_last_year = downloads_last_year + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET downloads_last_year = downloads_last_year + %i WHERE community_id = '%s'" %(results_num_found, community_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_total = downloads_total + %i WHERE community_id = '%s'" %(results_num_found, community_id)))
                    cursor.execute("UPDATE community_stats SET downloads_total = downloads_total + %i WHERE community_id = '%s'" %(results_num_found, community_id))

            # Commit changes
            db.commit()