"""Class for indexing a repository"""

from lib.database import Database
from dspace_reports.indexer import Indexer


class RepositoryIndexer(Indexer):
    """Class for indexing a repository"""

    def index(self):
        """Index function"""

        self.logger.info("Loading DSpace repository...")
        self.index_repository()

    def index_repository(self):
        """Index the entire repository"""

        # Get repository information
        repository_uuid = 0
        repository_name = "Unknown"

        site = self.rest.get_site()
        if 'uuid' in site:
            repository_uuid = site['uuid']

        if 'name' in site:
            repository_name = site['name']

        self.logger.info("Indexing Repository: %s (UUID: %s)", repository_name, repository_uuid)

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.debug(cursor.mogrify("INSERT INTO repository_stats (repository_id, repository_name) VALUES (%s, %s)", (repository_uuid, repository_name)))
                cursor.execute("INSERT INTO repository_stats (repository_id, repository_name) VALUES (%s, %s)", (repository_uuid, repository_name))

                db.commit()

        # Index views and downloads for the current community
        for time_period in self.time_periods:
            self.logger.info("Indexing repository items.")
            self.index_repository_items(repository_uuid=repository_uuid, time_period=time_period)

            self.logger.info("Indexing repository views.")
            self.index_repository_views(repository_uuid=repository_uuid, time_period=time_period)

            self.logger.info("Indexing repository downloads.")
            self.index_repository_downloads(repository_uuid=repository_uuid, time_period=time_period)

    def index_repository_items(self, repository_uuid=None, time_period=None):
        """Index repository items"""

        if repository_uuid is None or time_period is None:
            return

        # Create base Solr URL
        solr_url = self.solr_server + "/search/select"
        self.logger.debug("Solr_URL: %s", solr_url)

        # Default Solr params
        solr_query_params = {
            "q": "search.resourcetype:Item",
            "start": "0",
            "rows": "0",
            "wt": "json"
        }

        # Get date range for Solr query if time period is specified
        date_range = []
        date_range = self.get_date_range(time_period)
        if len(date_range) == 2:
            self.logger.info("Searching date range: %s - %s", date_range[0], date_range[1])
            if date_range[0] is not None and date_range[1] is not None:
                date_start = date_range[0]
                date_end = date_range[1]
                solr_query_params["fq"] = f"dc.date.accessioned_dt:[{date_start} TO {date_end}]"
        else:
            self.logger.error("Error creating date range.")

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total items in repository: %s", response.url)

        results_total_items = 0
        try:
            # Get total number of items
            results_total_items = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_total_items))
        except TypeError:
            self.logger.info("No items to index, returning.")
            return

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_last_month = %s WHERE repository_id = %s", (results_total_items, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET items_last_month = %s WHERE repository_id = %s", (results_total_items, repository_uuid))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_academic_year = %s WHERE repository_id = %s", (results_total_items, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET items_academic_year = %s WHERE repository_id = %s", (results_total_items, repository_uuid))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET items_total = %s WHERE repository_id = %s", (results_total_items, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET items_total = %s WHERE repository_id = %s", (results_total_items, repository_uuid))

                # Commit changes
                db.commit()

    def index_repository_views(self, repository_uuid=None, time_period=None):
        """Index repository views"""

        if repository_uuid is None or time_period is None:
            return

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Default Solr params
        solr_query_params = {
            "q": "type:2",
            "fq": "-isBot:true AND statistics_type:view",
            "shards": shards,
            "rows": 0,
            "wt": "json"
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
        self.logger.info("Calling Solr total item views in repository: %s", response.url)

        results_num_found = 0
        try:
            # Get total number of items
            results_num_found = response.json()["response"]["numFound"]
            self.logger.info("Solr repository - total item views: %s", str(results_num_found))
        except TypeError:
            self.logger.info("No item views to index.")
            return

        self.logger.info("Total repository item views: %s", str(results_num_found))

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                self.logger.info("Setting repository views stats with %s views for time period: %s",
                                 str(results_num_found), time_period)
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_last_month = %s WHERE repository_id = %s", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET views_last_month = %s WHERE repository_id = %s", (results_num_found, repository_uuid))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_academic_year = %s WHERE repository_id = %s", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET views_academic_year = %s WHERE repository_id = %s", (results_num_found, repository_uuid))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET views_total = %s WHERE repository_id = %s", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET views_total = %s WHERE repository_id = %s", (results_num_found, repository_uuid))

                # Commit changes
                db.commit()

    def index_repository_downloads(self, repository_uuid=None, time_period=None):
        """Index repository downloads"""

        if repository_uuid is None or time_period is None:
            return

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Default Solr params
        solr_query_params = {
            "q": "type:0",
            "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
            "shards": shards,
            "rows": 0,
            "wt": "json"
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
        self.logger.info("Calling Solr total item downloads in repository: %s", response.url)

        results_num_found = 0
        try:
            # Get total number of items
            results_num_found = response.json()["response"]["numFound"]
            self.logger.info("Solr repository - total item downloads: %s", str(results_num_found))
        except TypeError:
            self.logger.info("No item downloads to index.")
            return

        self.logger.info("Total repository item downloads: %s", str(results_num_found))

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_last_month = downloads_last_month + %s WHERE repository_id = %s", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET downloads_last_month = downloads_last_month + %s WHERE repository_id = %s", (results_num_found, repository_uuid))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_academic_year = downloads_academic_year + %s WHERE repository_id = %s", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET downloads_academic_year = downloads_academic_year + %s WHERE repository_id = %s", (results_num_found, repository_uuid))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE repository_stats SET downloads_total = downloads_total + %s WHERE repository_id = %s'", (results_num_found, repository_uuid)))
                    cursor.execute("UPDATE repository_stats SET downloads_total = downloads_total + %s WHERE repository_id = %s", (results_num_found, repository_uuid))

                # Commit changes
                db.commit()
