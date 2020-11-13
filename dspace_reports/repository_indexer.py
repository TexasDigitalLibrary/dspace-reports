from lib.database import Database
from dspace_reports.indexer import Indexer


class RepositoryIndexer(Indexer):
    def index(self):
        self.logger.info("Loading DSpace repository...")
        self.index_repository()

    def index_repository(self):
        # Get site hierarchy
        hierarchy = self.rest.get_hierarchy()

        # Repository information
        repository_id = hierarchy['id']
        repository_name = hierarchy['name']

        self.logger.info("Indexing Repository: %s (%s)" %(repository_name, repository_id))

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.debug(cursor.mogrify("INSERT INTO repository_stats (repository_id, repository_name) VALUES (%s, %s)", (repository_id, repository_name)))
                cursor.execute("INSERT INTO repository_stats (repository_id, repository_name) VALUES (%s, %s)", (repository_id, repository_name))
                
                db.commit()

        # Index views and downloads for the current community
        for time_period in self.time_periods:
            self.logger.info("Indexing repository items.")
            self.index_repository_items(repository_id=repository_id, time_period=time_period)

            self.logger.info("Indexing repository views.")
            self.index_repository_views(repository_id=repository_id, time_period=time_period)

            self.logger.info("Indexing repository downloads.")
            self.index_repository_downloads(repository_id=repository_id, time_period=time_period)
                
    def index_repository_items(self, repository_id=None, time_period=None):
        if repository_id is None or time_period is None:
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
        if time_period != 'all':
            date_range = []
            date_range = self.get_date_range(time_period)
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params["fq"] = f"dc.date.accessioned_dt:[{date_start} TO {date_end}]"
            else:
                self.logger.error("Error creating date range.")

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total items in repository: %s", response.url)
        
        results_totalItems = 0
        try:
            # Get total number of items
            results_totalItems = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_totalItems))
        except TypeError:
            self.logger.info("No items to index, returning.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_last_month = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id)))
                    cursor.execute("UPDATE repository_stats SET items_last_month = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_last_year = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id)))
                    cursor.execute("UPDATE repository_stats SET items_last_year = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_total = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id)))
                    cursor.execute("UPDATE repository_stats SET items_total = %i WHERE repository_id = '%s'" %(results_totalItems, repository_id))

                # Commit changes
                db.commit()

    def index_repository_views(self, repository_id=None, time_period=None):
        if repository_id is None or time_period is None:
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

        # Item views count
        total_repository_views = 0

        # Calculate pagination
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        while results_current_page <= results_num_pages:
            self.logger.info("Indexing repository views (page %s of %s)" %(str(results_current_page + 1), str(results_num_pages + 1)))

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

            # Get date range for Solr query if time period is specified
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

            # Make call to Solr for views statistics
            response = self.solr.call(url=solr_url, params=solr_query_params)
            self.logger.debug("Calling page %s of Solr item views in repository: %s" %(str(results_current_page +1), response.url))

            # Solr returns facets as a dict of dicts (see json.nl parameter)
            views = response.json()["facet_counts"]["facet_fields"]
            self.logger.info('Items in this batch : %s', str(len(views["id"].items())))

            # Loop through list of item views
            for item_id, item_views in views["id"].items():
                self.logger.info("Updating repository views total with %s views from item: %s" %(str(item_views), item_id))
                total_repository_views = total_repository_views + item_views

            results_current_page += 1

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.info("Setting repository views stats with %s views for time period: %s" %(str(total_repository_views), time_period))
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_last_month = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id)))
                    cursor.execute("UPDATE repository_stats SET views_last_month = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_last_year = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id)))
                    cursor.execute("UPDATE repository_stats SET views_last_year = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_total = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id)))
                    cursor.execute("UPDATE repository_stats SET views_total = %i WHERE repository_id = '%s'" %(total_repository_views, repository_id))

                # Commit changes
                db.commit()


    def index_repository_downloads(self, repository_id=None, time_period=None):
        if repository_id is None or time_period is None:
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
        self.logger.info("Calling Solr total item downloads in repository: %s", response.url)

        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No item downloads to index, returning.")
            return

        # Item downloads count
        total_repository_downloads = 0

        # Calculate pagination
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        while results_current_page <= results_num_pages:
            self.logger.info("Indexing repository downloads (page %s of %s)" %(str(results_current_page + 1), str(results_num_pages + 1)))

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

            # Get date range for Solr query if time period is specified
            if len(date_range) == 2:
                self.logger.info("Searching date range: %s - %s" %(date_range[0], date_range[1]))
                if date_range[0] is not None and date_range[1] is not None:
                    date_start = date_range[0]
                    date_end = date_range[1]
                    solr_query_params['q'] = solr_query_params['q'] + " AND " + f"time:[{date_start} TO {date_end}]"

            # Make call to Solr for downloads statistics
            response = self.solr.call(url=solr_url, params=solr_query_params)
            self.logger.debug("Calling page %s of Solr item downloads in repository: %s" %(str(results_current_page +1), response.url))

            # Solr returns facets as a dict of dicts (see json.nl parameter)
            downloads = response.json()["facet_counts"]["facet_fields"]
            
            # Loop through list of item downloads
            for item_id, item_downloads in downloads["owningItem"].items():
                self.logger.info("Updating repository downloads stats with %s views from item: %s" %(str(item_downloads), item_id))
                total_repository_downloads = total_repository_downloads + item_downloads

            results_current_page += 1
                    
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_last_month = downloads_last_month + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id)))
                    cursor.execute("UPDATE repository_stats SET downloads_last_month = downloads_last_month + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_last_year = downloads_last_year + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id)))
                    cursor.execute("UPDATE repository_stats SET downloads_last_year = downloads_last_year + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_total = downloads_total + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id)))
                    cursor.execute("UPDATE repository_stats SET downloads_total = downloads_total + %i WHERE repository_id = '%s'" %(total_repository_downloads, repository_id))

                # Commit changes
                db.commit()