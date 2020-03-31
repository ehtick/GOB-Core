from gobcore.datastore.datastore import Datastore, ORACLE, POSTGRES, OBJECTSTORE, WFS, FILE
from gobcore.datastore.oracle import OracleDatastore
from gobcore.datastore.postgres import PostgresDatastore
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.wfs import WfsDatastore
from gobcore.datastore.file import FileDatastore


class DatastoreFactory:

    @staticmethod
    def get_datastore(config: dict, read_config: dict = None) -> Datastore:
        store_type = config.pop('type')

        if store_type == ORACLE:
            return OracleDatastore(config, read_config)
        elif store_type == POSTGRES:
            return PostgresDatastore(config, read_config)
        elif store_type == OBJECTSTORE:
            return ObjectDatastore(config, read_config)
        elif store_type == WFS:
            return WfsDatastore(config, read_config)
        elif store_type == FILE:
            return FileDatastore(config, read_config)
        else:
            raise NotImplementedError
