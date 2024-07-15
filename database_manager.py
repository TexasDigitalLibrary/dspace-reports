"""Class for managing database functions"""

import argparse
import sys

from lib.database import Database
from lib.util import Utilities


class DatabaseManager():
    """Class for managing database functions"""
    repository_column_map = {
        'repository_id': 'Repository ID',
        'repository_name': 'Repository',
        'items_last_month': 'Items added last month',
        'items_academic_year': 'Items added in academic year',
        'items_total': 'Total Items',
        'views_last_month': 'Item views last month',
        'views_academic_year': 'Item views in academic year',   
        'views_total': 'Total item views',
        'downloads_last_month': 'Item downloads last month',
        'downloads_academic_year': 'Item downloads in academic year',
        'downloads_total': 'Total item downloads'
    }

    communities_column_map = {
        'community_id': 'Community ID',
        'community_name': 'Community Name',
        'community_url': 'Community URL',
        'parent_community_name': 'Parent Community Name',
        'items_last_month': 'Items added last month',
        'items_academic_year': 'Items added in academic year',
        'items_total': 'Total Items',
        'views_last_month': 'Item views last month',
        'views_academic_year': 'Item views in academic year',
        'views_total': 'Total item views',
        'downloads_last_month': 'Item downloads last month',
        'downloads_academic_year': 'Item downloads in academic year',
        'downloads_total': 'Total item downloads'
    }

    collections_column_map = {
        'parent_community_name': 'Parent Community Name',
        'collection_id': 'Collection ID',
        'collection_name': 'Collection Name',
        'collection_url': 'Collection URL',
        'items_last_month': 'Items added last month',
        'items_academic_year': 'Items added in academic year',
        'items_total': 'Total Items',
        'views_last_month': 'Item views last month',
        'views_academic_year': 'Item views in academic year',
        'views_total': 'Total item views',
        'downloads_last_month': 'Item downloads last month',
        'downloads_academic_year': 'Item downloads in academic year',
        'downloads_total': 'Total item downloads'
    }

    items_column_map = {
        'item_id': 'Item ID',
        'collection_name': 'Collection Name',
        'item_name': 'Item Title',
        'item_url': 'Item URL',
        'views_last_month': 'Item views last month',
        'views_academic_year': 'Item views in academic year',
        'views_total': 'Total item views',
        'downloads_last_month': 'Item downloads last month',
        'downloads_academic_year': 'Item downloads in academic year',
        'downloads_total': 'Total item downloads'
    }

    def __init__(self, config=None):
        if config is None:
            print('A configuration file required to create the community stats indexer.')
            return

        self.config = config

    def create_tables(self, config, logger):
        """Function to create statistics tables"""
        logger.info('Creating tables...')

        # Create statistics tables
        with Database(config=config['statistics_db']) as db:
            with db.cursor() as cursor:
                # Create new statistics tables
                commands = (
                    """
                    CREATE TABLE repository_stats (
                        repository_id UUID PRIMARY KEY NOT NULL,
                        repository_name VARCHAR(255) NOT NULL,
                        items_last_month INTEGER DEFAULT 0,
                        items_academic_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_academic_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_academic_year INTEGER DEFAULT 0,
                        downloads_total INTEGER DEFAULT 0
                    )
                    """,
                    """
                    CREATE TABLE community_stats (
                        community_id UUID PRIMARY KEY NOT NULL,
                        community_name VARCHAR(255) NOT NULL,
                        community_url VARCHAR(255) NOT NULL,
                        parent_community_name VARCHAR(255),
                        items_last_month INTEGER DEFAULT 0,
                        items_academic_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_academic_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_academic_year INTEGER DEFAULT 0,
                        downloads_total INTEGER DEFAULT 0
                    )
                    """,
                    """
                    CREATE TABLE collection_stats (
                        parent_community_name VARCHAR(255) NOT NULL,
                        collection_id UUID PRIMARY KEY NOT NULL,
                        collection_name VARCHAR(255) NOT NULL,
                        collection_url VARCHAR(255) NOT NULL,
                        items_last_month INTEGER DEFAULT 0,
                        items_academic_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_academic_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_academic_year INTEGER DEFAULT 0,
                        downloads_total INTEGER DEFAULT 0
                    )
                    """,
                    """
                    CREATE TABLE item_stats (
                        collection_name VARCHAR(255) NOT NULL,
                        item_id UUID PRIMARY KEY NOT NULL,
                        item_name VARCHAR(255) NOT NULL,
                        item_url VARCHAR(255) NOT NULL,
                        views_last_month INTEGER DEFAULT 0,
                        views_academic_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_academic_year INTEGER DEFAULT 0,
                        downloads_total INTEGER DEFAULT 0
                    )
                    """
                )

                for command in commands:
                    cursor.execute(command)

            # Commit changes
            db.commit()

        logger.info('Finished creating tables.')

    def drop_tables(self, config, logger):
        """Function to drop statistics tables"""
        # First check that tables exist
        tables_exist = self.check_tables(config, logger)
        if tables_exist is False:
            logger.info('Tables do not exist.')
            return

        logger.info('Dropping tables...')

        # Drop statistics tables
        with Database(config=config['statistics_db']) as db:
            with db.cursor() as cursor:
                # Create new statistics tables
                commands = (
                    """
                    DROP TABLE repository_stats
                    """,
                    """
                    DROP TABLE community_stats
                    """,
                    """
                    DROP TABLE collection_stats
                    """,
                    """
                    DROP TABLE item_stats
                    """
                )

                for command in commands:
                    cursor.execute(command)

            # Commit changes
            db.commit()

        logger.info('Finished dropping tables.')

    def check_tables(self, config, logger):
        """Function to check if statistics tables exist"""
        logger.debug('Checking for statistics tables.')
        tables_exist = False

        # Check if statistics tables exist
        with Database(config=config['statistics_db']) as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM information_schema.tables WHERE " +
                               "table_name='repository_stats'")
                if bool(cursor.rowcount):
                    logger.debug('The repository_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The repository_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE " +
                               "table_name='community_stats'")
                if bool(cursor.rowcount):
                    logger.debug('The community_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The community_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE " +
                               "table_name='collection_stats'")
                if bool(cursor.rowcount):
                    logger.debug('The collection_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The collection_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE " +
                               "table_name='item_stats'")
                if bool(cursor.rowcount):
                    logger.debug('The item_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The item_stats table DOES NOT exist.')
            # Commit changes
            db.commit()

        return tables_exist


def main():
    """Main function"""

    parser = argparse.ArgumentParser(
                    prog='Database Manager',
                    description='Commands to manage statistics database tables')

    parser.add_argument("-c", "--config", dest="config_file", action='store', type=str,
                        default="config/application.yml", help="Configuration file")
    parser.add_argument("-f", "--function", dest="function", action='store', type=str,
                        help="Database function to perform. Options: create, drop, check," +
                         " recreate.")

    args = parser.parse_args()

    # Create utilities object
    utilities = Utilities()

    # Check required options fields
    if args.function is None:
        parser.print_help()
        parser.error("Must specify a function to perform.")

    if args.function not in ['create', 'drop', 'check', 'recreate']:
        parser.print_help()
        parser.error("Must specify a valid function.")

    # Load config
    print("Loading configuration from file: %s", args.config_file)
    config = utilities.load_config(args.config_file)
    if not config:
        print("Unable to load configuration.")
        sys.exit(0)

    # Set up logging
    logger = utilities.load_logger(config=config)

    # Create object to manage database
    manage_database = DatabaseManager(config=config)

    # Perform function from command line
    if args.function == 'create':
        tables_exist = manage_database.check_tables(config, logger)
        if tables_exist is True:
            logger.error("Unable to create statistics tables because one or more (check logs) " +
                         "already exists.")
            sys.exit(0)
        logger.info('Creating statistics tables in the database.')
        manage_database.create_tables(config, logger)
    elif args.function == 'drop':
        logger.info('Dropping statistics tables')
        manage_database.drop_tables(config, logger)
    elif args.function == 'check':
        logger.info('Checking for statistics tables.')
        tables_exist = manage_database.check_tables(config, logger)
        if tables_exist is True:
            logger.info('One or more statistics tables exists (check logs).')
            sys.exit(0)
    elif args.function == 'recreate':
        logger.info('Droping and recreating statistics tables in the database.')
        manage_database.drop_tables(config, logger)
        manage_database.create_tables(config, logger)


if __name__ == "__main__":
    main()
