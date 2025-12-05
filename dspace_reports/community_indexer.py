"""Class for indexing communities"""

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
            with Database(self.config['database']) as db:
                with db.cursor() as cursor:
                    self.logger.debug(cursor.mogrify("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_uuid, community_name, community_url, parent_community_name)))
                    cursor.execute("INSERT INTO community_stats (community_id, community_name, community_url, parent_community_name) VALUES (%s, %s, %s, %s)", (community_uuid, community_name, community_url, parent_community_name))
                    db.commit()

            for time_period in self.time_periods:
                self.logger.info("Indexing items for community: %s (%s)", community_name,
                                 community_uuid)
                self.index_community_items(community_uuid=community_uuid, time_period=time_period)

                # Index all views and downloads of communities
                self.logger.info("Updating views statistics for community during time period: %s",
                             time_period)
                self.index_community_views(community_uuid=community_uuid, time_period=time_period)

                self.logger.info("Updating downloads statistics for community during time period: %s",
                                time_period)
                self.index_community_downloads(community_uuid=community_uuid, time_period=time_period)

    def index_community_items(self, community_uuid=None, time_period=None):
        """Index the community items"""

        if community_uuid is None or time_period is None:
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

        # Add community UUID to query parameter
        solr_query_params['q'] = solr_query_params['q'] + " AND location.comm:" + community_uuid

        # Make call to Solr for items statistics
        response = self.solr.query_search(params=solr_query_params)
        self.logger.info("Calling Solr items in community: %s", response.url)

        results_total_items = 0
        try:
            # Get total number of items
            results_total_items = response.json()["response"]["numFound"]
            self.logger.info("Solr - total items: %s", str(results_total_items))
        except TypeError:
            self.logger.info("No community items to index.")
            return

        with Database(self.config['database']) as db:
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

    def index_community_views(self, community_uuid=None, time_period=None):
        """Index the community views"""

        if community_uuid is None or time_period is None:
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

        print(f"Indexing community views for community: {community_uuid} " +
              "during time: {solr_date_string}.")

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Update database
        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                # Solr params
                solr_query_params = {
                    "q": f"owningComm:({community_uuid})",
                    "fq": f"type:2 AND -isBot:true AND statistics_type:view AND {solr_date_string}",
                    "fl": "owningComm",
                    "facet": "true",
                    "facet.field": "owningComm",
                    "facet.mincount": 1,
                    "shards": shards,
                    "rows": 0,
                    "wt": "json",
                    "json.nl": "map",  # return facets as a dict instead of a flat list
                }

                response = self.solr.query_statistics(params=solr_query_params)
                self.logger.info("Solr community views query: %s", response.url)

                # Solr returns facets as a dict of dicts (see json.nl parameter)
                views = response.json()["facet_counts"]["facet_fields"]

                self.logger.debug("Found %s results in Solr", len(views))

                # Iterate over the facetField dict and get the UUIDs and views
                for community_id, community_views in views["owningComm"].items():
                    if community_id == community_uuid:
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_last_month = %s WHERE community_id = %s", (community_views, community_uuid)))
                            cursor.execute("UPDATE community_stats SET views_last_month = %s WHERE community_id = %s", (community_views, community_uuid))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_academic_year = %s WHERE community_id = %s", (community_views, community_uuid)))
                            cursor.execute("UPDATE community_stats SET views_academic_year = %s WHERE community_id = %s", (community_views, community_uuid))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET views_total = %s WHERE community_id = %s", (community_views, community_uuid)))
                            cursor.execute("UPDATE community_stats SET views_total = %s WHERE community_id = %s", (community_views, community_uuid))
                    
                        # Commit changes to database
                        db.commit()
                    else:
                        self.logger.warning("Solr query returned results for a different community UUID: %s", community_id)

    def index_community_downloads(self, community_uuid=None, time_period=None):
        """Index the community downloads"""

        if community_uuid is None or time_period is None:
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

        print(f"Indexing community downloads for community: {community_uuid} " +
              "during time: {solr_date_string}.")

        # Get Solr shards
        shards = self.solr.get_statistics_shards()

        # Update database
        with Database(self.config['database']) as db:
            with db.cursor() as cursor:
                # Solr params
                solr_query_params = {
                    "q": f"owningComm:({community_uuid})",
                    "fq": f"type:0 AND -isBot:true AND statistics_type:view AND bundleName:ORIGINAL AND {solr_date_string}",
                    "fl": "owningComm",
                    "facet": "true",
                    "facet.field": "owningComm",
                    "facet.mincount": 1,
                    "shards": shards,
                    "rows": 0,
                    "wt": "json",
                    "json.nl": "map",  # return facets as a dict instead of a flat list
                }

                response = self.solr.query_statistics(params=solr_query_params)
                self.logger.info("Solr community downloads query: %s", response.url)

                # Solr returns facets as a dict of dicts (see json.nl parameter)
                downloads = response.json()["facet_counts"]["facet_fields"]

                self.logger.debug("Found %s results in Solr", len(downloads))

                # Iterate over the facetField dict and get the UUIDs and views
                for community_id, community_downloads in downloads["owningComm"].items():
                    if community_id == community_uuid:
                        if time_period == 'month':
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_last_month = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                            cursor.execute("UPDATE community_stats SET downloads_last_month = %s WHERE community_id = %s", (community_downloads, community_uuid))
                        elif time_period == 'year':
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_academic_year = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                            cursor.execute("UPDATE community_stats SET downloads_academic_year = %s WHERE community_id = %s", (community_downloads, community_uuid))
                        else:
                            self.logger.debug(cursor.mogrify("UPDATE community_stats SET downloads_total = %s WHERE community_id = %s", (community_downloads, community_uuid)))
                            cursor.execute("UPDATE community_stats SET downloads_total = %s WHERE community_id = %s", (community_downloads, community_uuid))
                    
                        # Commit changes to database
                        db.commit()
                    else:
                        self.logger.warning("Solr query returned results for a different community UUID: %s", community_id)
