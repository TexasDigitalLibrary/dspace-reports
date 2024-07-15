"""Class for interacting with a DSpace 7+ database"""

import logging
import psycopg


class Database():
    """Class for interacting with a DSpace 7+ database"""

    def __init__(self, config):
        self.config = config
        self._connection_uri = f"dbname={config['name']} user={config['username']} password={config['password']} host={config['host']} port={config['port']}"
        self.logger = logging.getLogger('dspace-reports')

    def __enter__(self):
        try:
            self._connection = psycopg.connect(
                self._connection_uri, cursor_factory=psycopg.ClientCursor
            )
        except psycopg.OperationalError as err:
            self.logger.error("Cannot connect to database. Please check connection information.")
            self.logger.error("Error: %s, %s", err, type(err))

        return self._connection

    def create_connection(self):
        """Create database connection"""

        # Debug information
        self.logger.info("Attempting to connect to Dataverse database: %s (host), %s (database)," +
                         " %s (username) ******** (password).", self.config['host'],
                         self.config['name'], self.config['username'])

        # Create connection to database
        try:
            self.connection = psycopg.connect(self._connection_uri,
                                              cursor_factory=psycopg.ClientCursor)
            return True
        except psycopg.OperationalError as err:
            self.logger.error("Cannot connect to database. Please check connection information.")
            self.logger.error("Error: %s, %s", err, type(err))
            return False

    def close_connection(self):
        """Close database connection"""
        self._connection.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._connection.close()
