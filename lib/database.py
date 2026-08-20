"""Class for interacting with a DSpace 7+ database"""

import logging
import psycopg


class Database():
    """Class for interacting with a DSpace 7+ database"""

    def __init__(self, config):
        self.logger = logging.getLogger('dspace-reports')
        self.config = config
        self._connection_uri = (
            f"dbname={config['name']} user={config['username']} password={config['password']} "
            f"host={config['host']} port={config['port']}"
        )
        self._connection = None

    def __enter__(self):
        try:
            self._connection = psycopg.connect(
                self._connection_uri, cursor_factory=psycopg.ClientCursor
            )
        except psycopg.OperationalError as err:
            self.logger.error("Cannot connect to database. Please check connection information.")
            self.logger.error("Error: %s, %s", err, type(err))

        return self._connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._connection:
            if exc_type is not None:
                self._connection.rollback()
            else:
                self._connection.commit()

            self._connection.close()

        return False
