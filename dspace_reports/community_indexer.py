"""Class for indexing communities"""

import math

from lib.database import Database
from dspace_reports.indexer import Indexer


class CommunityIndexer(Indexer):
    """Class for indexing communities"""

    def index(self):
        """Index function"""

        self.logger.info("Loading DSpace communities...")
        self.index_communities()

    def index_communities(self):
        """Index the communities in the repository"""

        # Get a list of all communities from the REST API
        communities = self.rest.get_communities()
        for community in communities:
            community_uuid = community['uuid']
            community_name = community['name']
            self.logger.info("Loading community: %s (%s)...", community_name, community_uuid)

            # Get community metadata, including parent community name
            community_handle = community['handle']
            community_url = self.base_url + community_handle

            parent_community_name = ""
            parent_community = self.rest.get_community_parent_community(
                community_uuid=community_uuid)
            if parent_community is not None and 'name' in parent_community:
                parent_community_name = parent_community['name']

            if len(community_name) > 255:
                self.logger.debug("Community name is longer than 255 characters. " +
                                  "It will be shortened to that length.")
                community_name = community_name[0:251] + "..."

            # Insert the community into the database
            with Database(self.config['statistics_db']) as db:
                with db.cursor() as cursor:
                    self.logger.debug(cursor.mogrify("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_uuid, community_name, community_url, parent_community_name)))
                    cursor.execute("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_uuid, community_name, community_url, parent_community_name))
                    db.commit()

            for time_period in self.time_periods:
                self.logger.info("Indexing items for community: %s (%s)", community_name,
                                 community_uuid)
                self.index_community_items(community_uuid=community_uuid, time_period=time_period)

        # Index all views and downloads of communities
        for time_period in self.time_periods:
            self.logger.info("Updating views statistics for communities during time period: %s",
                             time_period)
            self.index_community_views(time_period=time_period)

            self.logger.info("Updating downloads statistics for communities during time period: %s",
                             time_period)
            self.index_community_downloads(time_period=time_period)

    def index_community_items(self, community_uuid=None, time_period=None):
        """Index the community items"""

        if community_uuid is None or time_period is None:
            return None

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

        # Add community UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND location.comm:" + community_uuid

        # Make call to Solr for items statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr items in community: %s", response.url)

        results_total_items = 0
        try:
            # Get total number of items
            results_total_items = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_total_items))
        except TypeError:
            self.logger.info("No community items to index.")
            return None

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                if time_period == 'month':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_last_month = %s WHERE community_id = %s", (results_total_items, community_uuid)))
                    cursor.execute("UPDATE community_stats SET items_last_month = %s WHERE community_id = %s", (results_total_items, community_uuid))
                elif time_period == 'year':
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_academic_year = %s WHERE community_id = %s", (results_total_items, community_uuid)))
                    cursor.execute("UPDATE community_stats SET items_academic_year = %s WHERE community_id = %s", (results_total_items, community_uuid))
                else:
                    self.logger.debug(cursor.mogrify("UPDATE community_stats SET items_total = %s WHERE community_id = %s", (results_total_items, community_uuid)))
                    cursor.execute("UPDATE community_stats SET items_total = %s WHERE community_id = %s", (results_total_items, community_uuid))

                # Commit changes
                db.commit()

    def index_community_views(self, time_period=None):
        """Index the community views"""

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Default Solr params
        solr_query_params = {
            "q": f"type:0 AND owningComm:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view",
            "fl": "owningComm",
            "facet": "true",
            "facet.field": "owningComm",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "owningComm",
            "stats.calcdistinct": "true",
            "shards": shards,
            "rows": 0,
            "wt": "json",
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
                solr_query_params['q'] = (solr_query_params['q'] + " AND " +
                                          f"time:[{date_start} TO {date_end}]")
            else:
                self.logger.error("Error creating date range.")
        else:
            self.logger.error("Error creating date range.")

        # Make call to Solr for views statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total community views in communities: %s", response.url)

        try:
            # get total number of distinct facets (countDistinct)
            results_total_num_facets = response.json()["stats"]["stats_fields"]["owningComm"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No community views to index.")
            return

        # divide results into "pages" and round up to next integer
        results_per_page = 100
        results_num_pages = math.ceil(results_total_num_facets / results_per_page)
        results_current_page = 0

        # Update database
        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    print(
                        f"Indexing community views (page {results_current_page + 1} " +
                        f"of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:2 AND owningComm:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view",
                        "fl": "owningComm",
                        "facet": "true",
                        "facet.field": "owningComm",
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

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr community views query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and views
                    for community_uuid, community_views in views["owningComm"].items():
                        if len(community_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_last_month = %s WHERE community_id = %s", (community_views, community_uuid)))
                                cursor.execute("UPDATE community_stats SET views_last_month = %s WHERE community_id = %s", (community_views, community_uuid))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_academic_year = %s WHERE community_id = %s", (community_views, community_uuid)))
                                cursor.execute("UPDATE community_stats SET views_academic_year = %s WHERE community_id = %s", (community_views, community_uuid))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_total = %s WHERE community_id = %s", (community_views, community_uuid)))
                                cursor.execute("UPDATE community_stats SET views_total = %s WHERE community_id = %s", (community_views, community_uuid))
                        else:
                            self.logger.warning("owningComm value is not a UUID: %s",
                                                community_uuid)

                    # Commit changes to database
                    db.commit()

                    results_current_page += 1

    def index_community_downloads(self, time_period=None):
        """Index the community downloads"""

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Default Solr params
        solr_query_params = {
            "q": f"type:0 AND owningComm:/.{{36}}/",
            "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
            "fl": "owningComm",
            "facet": "true",
            "facet.field": "owningComm",
            "facet.mincount": 1,
            "facet.limit": 1,
            "facet.offset": 0,
            "stats": "true",
            "stats.field": "owningComm",
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

        # Make call to Solr for downloads statistics
        response = self.solr.call(url=solr_url, params=solr_query_params)
        self.logger.info("Calling Solr total community downloads in community: %s", response.url)

        try:
            # get total number of distinct facets (countDistinct)
            results_total_num_facets = response.json()["stats"]["stats_fields"]["owningComm"][
                "countDistinct"
            ]
        except TypeError:
            self.logger.info("No community downloads to index.")
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
                        f"Indexing community downloads (page {results_current_page + 1} " +
                        f"of {results_num_pages + 1})"
                    )

                    # Solr params for current page
                    solr_query_params = {
                        "q": f"type:0 AND owningComm:/.{{36}}/",
                        "fq": "-isBot:true AND statistics_type:view AND bundleName:ORIGINAL",
                        "fl": "owningComm",
                        "facet": "true",
                        "facet.field": "owningComm",
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

                    response = self.solr.call(url=solr_url, params=solr_query_params)
                    self.logger.info("Solr community downloads query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = response.json()["facet_counts"]["facet_fields"]
                    # Iterate over the facetField dict and get the UUIDs and downloads
                    for community_uuid, community_downloads in downloads["owningComm"].items():
                        if len(community_uuid) == 36:
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_last_month = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                                cursor.execute("UPDATE community_stats SET downloads_last_month = %s WHERE community_id = %s", (community_downloads, community_uuid))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_academic_year = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                                cursor.execute("UPDATE community_stats SET downloads_academic_year = %s WHERE community_id = %s", (community_downloads, community_uuid))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_total = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                                cursor.execute("UPDATE community_stats SET downloads_total = %s WHERE community_id = %s", (community_downloads, community_uuid))
                        else:
                            self.logger.warning("owningComm value is not a UUID: %s",
                                                community_uuid)

                    # Commit changes to database
                    db.commit()

                    results_current_page += 1
