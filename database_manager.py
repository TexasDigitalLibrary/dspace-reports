import sys

from optparse import OptionParser

from lib.database import Database
from lib.util import Utilities


class DatabaseManager():
    def __init__(self, config=None):
        if config is None:
            print('A configuration file required to create the community stats indexer.')
            return

        self.config = config

    def create_tables(self, config, logger):
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
                        items_last_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_last_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_last_year INTEGER DEFAULT 0,
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
                        items_last_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_last_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_last_year INTEGER DEFAULT 0,
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
                        items_last_year INTEGER DEFAULT 0,
                        items_total INTEGER DEFAULT 0,
                        views_last_month INTEGER DEFAULT 0,
                        views_last_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_last_year INTEGER DEFAULT 0,
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
                        views_last_year INTEGER DEFAULT 0,
                        views_total INTEGER DEFAULT 0,
                        downloads_last_month INTEGER DEFAULT 0,
                        downloads_last_year INTEGER DEFAULT 0,
                        downloads_total INTEGER DEFAULT 0
                    )
                    """
                )

                for command in commands:
                    cursor.execute(command)

            # Commit changes
            db.commit()

    def drop_tables(self, config, logger):
        # First check that tables exist
        tables_exist = self.check_tables(config, logger)
        if tables_exist == False:
            logger.info('Tables do not exist.')
            return
        else:
            logger.info('Removing tables...')

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

    def check_tables(self, config, logger):
        logger.debug('Checking for statistics tables.')
        tables_exist = False

        # Check if statistics tables exist
        with Database(config=config['statistics_db']) as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", ('repository_stats',))
                if bool(cursor.rowcount):
                    logger.debug('The repository_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The repository_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", ('community_stats',))
                if bool(cursor.rowcount):
                    logger.debug('The community_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The community_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", ('collection_stats',))
                if bool(cursor.rowcount):
                    logger.debug('The collection_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The collection_stats table DOES NOT exist.')
                cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", ('item_stats',))
                if bool(cursor.rowcount):
                    logger.debug('The item_stats table exists.')
                    tables_exist = True
                else:
                    logger.debug('The item_stats table DOES NOT exist.')
            # Commit changes
            db.commit()

        return tables_exist


def main():
    parser = OptionParser()

    parser.add_option("-c", "--config", dest="config_file", default="config/application.yml", help="Configuration file")
    parser.add_option("-f", "--function", dest="function", help="Database function to perform. Options: create, drop, check, recreate")

    (options, args) = parser.parse_args()

    # Create utilities object
    utilities = Utilities()

    # Check required options fields
    if options.function is None:
        parser.print_help()
        parser.error("Must specify a function to perform.")

    if options.function not in ['create', 'drop', 'check', 'recreate']:
        parser.print_help()
        parser.error("Must specify a valid function.")
    
    # Load config
    print("Loading configuration from file: %s" %(options.config_file))
    config = utilities.load_config(options.config_file)
    if not config:
        print("Unable to load configuration.")
        sys.exit(0)

    # Set up logging
    logger = utilities.load_logger(config=config)
    
    # Create object to manage database
    manage_database = DatabaseManager(config=config)

    # Perform function from command line
    if options.function == 'create':
        tables_exist = manage_database.check_tables(config, logger)
        if tables_exist == True:
            logger.error('Unable to create statistics tables because one or more (check logs) already exists.')
            sys.exit(0)
        logger.info('Creating statistics tables in the database.')
        manage_database.create_tables(config, logger)
    elif options.function == 'drop':
        logger.info('Dropping statistics tables')
        manage_database.drop_tables(config, logger)
    elif options.function == 'check':
        logger.info('Checking for statistics tables.')
        tables_exist = manage_database.check_tables(config, logger)
        if tables_exist == True:
            logger.info('One or more statistics tables exists (check logs).')
            sys.exit(0)
    elif options.function == 'recreate':
        logger.info('Droping and recreating statistics tables in the database.')
        manage_database.drop_tables(config, logger)
        manage_database.create_tables(config, logger)


if __name__ == "__main__":
    main()