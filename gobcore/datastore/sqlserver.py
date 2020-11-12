import pyodbc
import os

from typing import List

from gobcore.datastore.sql import SqlDatastore


SQLSERVER_ODBC_DRIVER = os.getenv('SQLSERVER_ODBC_DRIVER')


class SqlServerDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(SqlServerDatastore, self).__init__(connection_config, read_config)

    def connect(self):
        self.connection = pyodbc.connect(
            server=self.connection_config['host'],
            database=self.connection_config['database'],
            user=self.connection_config['username'],
            password=self.connection_config['password'],
            port=self.connection_config['port'],
            driver=SQLSERVER_ODBC_DRIVER
        )

    def query(self, query):
        cursor = self.connection.cursor()
        with self.connection:
            cursor.execute(query)

            for row in cursor.fetchall():
                # Convert tuple to dict
                yield {t[0]: row[i] for i, t in enumerate(row.cursor_description)}

    def write_rows(self, table: str, rows: List[list]) -> None:
        raise NotImplementedError(f"Please implement write_rows for {self.__class__}")

    def execute(self, query: str) -> None:
        raise NotImplementedError(f"Please implement execute for {self.__class__}")

    def list_tables_for_schema(self, schema: str) -> List[str]:
        raise NotImplementedError(f"Please implement write_rows for {self.__class__}")

    def rename_schema(self, schema: str, new_name: str) -> None:
        raise NotImplementedError(f"Please implement rename_schema for {self.__class__}")
