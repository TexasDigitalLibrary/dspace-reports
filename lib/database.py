import psycopg2
import psycopg2.extras
import logging

class Database(object):
    def __init__(self, config):
        self._connection_uri = f"dbname={config['name']} user={config['username']} password={config['password']} host={config['host']} port={config['port']}"
        self.logger = logging.getLogger('dspace-reports')

    def __enter__(self):
        try:
            self._connection = psycopg2.connect(
                self._connection_uri, cursor_factory=psycopg2.extras.DictCursor
            )
        except psycopg2.OperationalError:
            self.logger.error("Cannot connect to database. Please check connection information and try again.")

        return self._connection

    def create_connection(self):
        # Debug information
        self.logger.info("Attempting to connect to Dataverse database: %s (host), %s (database), %s (username) ******** (password).", self.host, self.database, self.username)

        # Create connection to database
        try:
            self.connection = psycopg2.connect(self._connection_uri)
            return True
        except Exception as e:
            self.logger.error("Cannot connect to database. Please check connection information and try again.")
            return False

    def close_connection(self):
        self._connection.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._connection.close()
