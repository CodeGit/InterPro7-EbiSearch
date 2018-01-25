__author__ = 'maq'

import logging

import pymysql.cursors

logger = logging.getLogger(__name__)

class DBConnection:
    """Context manager class for oracle DB connection"""

    def __init__(self, config):
        self.user = config["user"]
        self.password = config.get("password", None)
        self.host = config.get("host", None)
        self.port = config.get("port", None)
        self.schema = config["schema"]
        self.connection = None

    def __enter__(self):
        if (self.connection is not None):
            raise RuntimeError("Connection already exists")
        """
        connStr = self.user
        if (self.password is not None):
            connStr = connStr + "/" + self.password
        connStr = connStr + "@"

        if (self.host is not None and self.port is not None):
            connStr = connStr + self.host + ":" + self.port + "/"

        connStr = connStr + self.schema
        self.connection = pymysql.connect(connStr)
        """
        self.connection = pymysql.connect(
            host = self.host,
            port = int(self.port),
            user = self.user,
            passwd = self.password,
            db = self.schema,
            charset = 'utf8'
        )
        return self.connection

    def __exit__(self, ext_type, exc_value, traceback):
        if self.connection != None:
            self.connection.close()
            self.connection = None
        if exc_value != None and traceback != None:
            logger.error("DB error: %s [%s]", str(exc_value), traceback)

if __name__ == '__main__':
    pass