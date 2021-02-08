import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source.base import DataSource, Schema

from . import __version__


class MetabaseCatalog(Catalog):
    name = 'metabase_catalog'
    version = __version__
    # partition_access = False

    def __init__(self, domain, username, password, metadata=None):
        self.domain = domain
        self.username = username
        self.password = password

        self._metabase = MetabaseAPI(self.domain, self.username, self.password)

        super().__init__(name='metabase', metadata=metadata)

    def _load(self):
        databases = self._metabase.get_databases()

        self._entries = {}
        for db in databases:
            for table in db['tables']:
                e = LocalCatalogEntry(
                    name=table['name'],
                    description=table['description'],
                    driver=MetabaseTableSource,
                    catalog=self,
                    args={
                        'domain': self.domain,
                        'username': self.username,
                        'password': self.password,
                        'database': db['id'],
                        'table': table['id']
                    }
                )
                e._plugin = [MetabaseTableSource]
                # self._entries[db['name']][table['name']] = e
                self._entries[table['name']] = e


class MetabaseTableSource(DataSource):
    name = 'metabase_table'
    container = 'dataframe'
    version = __version__
    partition_access = True

    def __init__(self, domain, username, password, database, table, *kwargs, metadata=None):
        self.domain = domain
        self.username = username
        self.password = password
        self.database = database
        self.table = table
        self._df = None

        self._metabase = MetabaseAPI(self.domain, self.username, self.password)

        super(MetabaseTableSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            self._df = self._metabase.get_table(self.database, self.table)

        return Schema(datashape=None,
                      dtype=self._df,
                      shape=(None, len(self._df.columns)),
                      npartitions=1,
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._df

    def read(self):
        self._get_schema()
        return self._df

    def to_dask(self):
        raise NotImplementedError()

    def _close(self):
        self._dataframe = None


class MetabaseAPI():
    def __init__(self, domain, username, password):
        self.domain = domain
        self.password = password
        self.username = username
        self._token = None
        self._token_expiration = datetime.now()

    def _create_or_refresh_token(self):
        if self._token and (datetime.now() < self._token_expiration):
            return

        res = requests.post(
            urljoin(self.domain, '/api/session'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'username': self.username,
                'password': self.password
            })
        )
        res.raise_for_status()

        self._token = res.json()['id']
        self._token_expiration = datetime.now() + timedelta(days=10)

    def get_databases(self):
        self._create_or_refresh_token()

        headers = {
            'X-Metabase-Session': self._token
        }
        params = {'include': 'tables'}

        res = requests.get(
            urljoin(self.domain, '/api/database'),
            headers=headers, params=params
        )

        return res.json()

    def get_metadata(self, table):
        self._create_or_refresh_token()

        headers = {
            'X-Metabase-Session': self._token
        }

        res = requests.get(
            urljoin(self.domain, f'/api/table/{table}/query_metadata'),
            headers=headers
        )

        return res.json()

    def get_table(self, database, table):
        from io import StringIO

        import pandas as pd

        self._create_or_refresh_token()

        table_metadata = self.get_metadata(table)
        date_fields = [f['display_name'] for f in table_metadata['fields']
                       if 'date' in f['base_type'].lower()]

        body = {
            "database": database,
            "query": {"source-table": table},
            "type": "query",
            "middleware": {
                "js-int-to-string?": True,
                "add-default-userland-constraints?": True
            }
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Metabase-Session': self._token
        }

        res = requests.post(
            urljoin(self.domain, '/api/dataset/csv'),
            headers=headers,
            params={'query': json.dumps(body)}
        )

        res.raise_for_status()
        csv = res.text

        return pd.read_csv(StringIO(csv), parse_dates=date_fields, infer_datetime_format=True)
