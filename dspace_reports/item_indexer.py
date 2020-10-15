import logging
import re
import psycopg2.extras
import requests
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

from lib.database import Database
from lib.api import DSpaceRestApi
from dspace_reports.indexer import Indexer


class ItemIndexer(Indexer):
    def index(self):
        # Iterate over DSpace items to fill item_stats table
        items = self.rest.get_items()
        self.logger.debug("Found %s items.", str(len(items)))

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                for index in range(len(items)):
                    item = items[index]
                    self.logger.info("Loading item: %s", item['uuid'])
                    item_id = item['uuid']
                    item_name = item['name']
                    
                    # If name is null then use "Untitled"
                    if item_name is not None:
                        # If item name is longer than 255 characters then shorten it
                        if len(item_name) > 255:
                            item_name = item_name[0:251] + "..."
                    else:
                        item_name = "Untitled"    

                    # Create handle URL for item
                    item_url = self.base_url + item['handle']

                    self.logger.debug(cursor.mogrify("INSERT INTO item_stats (item_id, item_name, item_url) VALUES (%s, %s, %s)" %(item_id, item_name, item_url)))
                    cursor.execute("INSERT INTO item_stats (item_id, item_name, item_url) VALUES (%s, %s, %s)" %(item_id, item_name, item_url))
                    db.commit()


        for time_period in self.time_periods:
            self.logger.info("Indexing Solr views for time period: %s ", time_period)
            self.index_views(time_period=time_period)

            self.logger.info("Indexing Solr downloads for time period: %s ", time_period)
            self.index_downloads(time_period=time_period)

    def index_views(self, time_period='all'):
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
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

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get date range for Solr query if time period is specified
        date_range = str()
        date_range = self.get_date_range(time_period)
        if len(date_range) > 0:
            solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

        # Make call to Solr for views statistics
        response = requests.get(solr_url, params=solr_query_params)
        self.logger.info("Solr total item views query: %s", response.url)
        
        try:
            # Get total number of items
            results_totalNumFacets = response.json()["stats"]["stats_fields"]["id"][
                "countDistinct"
            ]
            self.logger.info("Solr total item views count: %s", str(results_totalNumFacets))
        except TypeError:
            self.logger.info("No item views to index, returning.")
            return

        # divide results into "pages" (cast to int to effectively round down)
        results_per_page = 100
        results_num_pages = int(results_totalNumFacets / results_per_page)
        results_current_page = 0

        with Database(self.config['statistics_db']) as db:
            with db.cursor() as cursor:
                while results_current_page <= results_num_pages:
                    self.logger.info("Indexing item views (page %s of %s" %(str(results_current_page + 1), str(results_num_pages + 1)))

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
                        "json.nl": "map",  # return facets as a dict instead of a flat list
                    }

                    # Create base Solr url
                    solr_url = self.solr_server + "/statistics/select"

                    # Add date range for Solr query if time period is specified
                    if len(date_range) > 0:
                        solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

                    # Make call to Solr for views statistics
                    response = requests.get(solr_url, params=solr_query_params)
                    self.logger.info("Solr total item downloads query: %s", response.url)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    views = response.json()["facet_counts"]["facet_fields"]
                    self.logger.info('items in this batch : %s', str(len(views["id"].items())))

                    # Loop through list of item views
                    for item_views, item_id in views["id"].items():
                        if self.validate_uuid4(item_id):
                            if time_period == 'month':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_id, str(item_views))))
                                cursor.execute("UPDATE item_stats SET views_last_month = %s WHERE item_id = %s", (item_id, str(item_views)))
                            elif time_period == 'year':
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_last_year = %s WHERE item_id = %s", (item_id, str(item_views))))
                                cursor.execute("UPDATE item_stats SET views_last_year = %s WHERE item_id = %s", (item_id, str(item_views)))
                            else:
                                self.logger.debug(cursor.mogrify("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_id, str(item_views))))
                                cursor.execute("UPDATE item_stats SET views_total = %s WHERE item_id = %s", (item_id, str(item_views)))
                        elif '-unmigrated' in item_id:
                            self.logger.debug("Item ID was not migrated to UUID: %s" %(item_id))
                        else:
                            self.logger.debug("Item ID is not valid: %s" %(item_id))
                            
                    # Commit changes
                    db.commit()

                    results_current_page += 1


    def index_downloads(self, time_period='all'):
        # Get Solr shards
        shards = self.solr.get_statistics_shards(time_period)

        # Solr params
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

        # Create base Solr url
        solr_url = self.solr_server + "/statistics/select"

        # Get date range for Solr query if time period is specified
        date_range = str()
        date_range = self.get_date_range(time_period)
        if len(date_range) > 0:
            solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

        # Make call to Solr for download statistics
        res = requests.get(solr_url, params=solr_query_params)
        self.logger.info("Solr total item downloads query: %s", res.url)

        try:
            # Get total number of items
            results_totalNumFacets = res.json()["stats"]["stats_fields"]["owningItem"][
                "countDistinct"
            ]
            self.logger.info("Solr total item downloads count: %s", str(results_totalNumFacets))
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
                    self.logger.info("Indexing item downloads (page %s of %s" %(str(results_current_page + 1), str(results_num_pages + 1)))

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

                    # Create base Solr url
                    solr_url = self.solr_server + "/statistics/select"

                    # Add date range for Solr query if time period is specified
                    if len(date_range) > 0:
                        solr_query_params['q'] = solr_query_params['q'] + " AND " + date_range

                    # Make call to Solr for views statistics
                    res = requests.get(solr_url, params=solr_query_params)

                    # Solr returns facets as a dict of dicts (see json.nl parameter)
                    downloads = res.json()["facet_counts"]["facet_fields"]
                    self.logger.info('Items in this batch : %s', str(len(downloads["owningItem"].items())))

                    # Loop through list of item downloads
                    for item_downloads, item_id in downloads["owningItem"].items():
                        if self.validate_uuid4(item_id):
                            if time_period == 'month':
                                self.logger.info(cursor.mogrify("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_id, str(item_downloads))))
                                cursor.execute("UPDATE item_stats SET downloads_last_month = %s WHERE item_id = %s", (item_id, str(item_downloads)))
                            elif time_period == 'year':
                                self.logger.info(cursor.mogrify("UPDATE item_stats SET downloads_last_year = %s WHERE item_id = %s", (item_id, str(item_downloads))))
                                cursor.execute("UPDATE item_stats SET downloads_last_year = %s WHERE item_id = %s", (item_id, str(item_downloads)))
                            else:
                                self.logger.info(cursor.mogrify("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_id, str(item_downloads))))
                                cursor.execute("UPDATE item_stats SET downloads_total = %s WHERE item_id = %s", (item_id, str(item_downloads)))
                        elif '-unmigrated' in item_id:
                            self.logger.debug("Item ID was not migrated to UUID: %s" %(item_id))
                        else:
                            self.logger.debug("Item ID is not valid: %s" %(item_id))

                    # Commit changes
                    db.commit()

                    results_current_page += 1
