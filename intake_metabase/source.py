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

    def __init__(self, domain, username=None, password=None, token=None, metadata=None, name=None):
        self.name = name
        self.domain = domain
        self.username = username
        self.password = password
        self.token = token

        self._metabase = MetabaseAPI(self.domain, self.username, self.password, self.token)

        super().__init__(name='metabase', metadata=metadata)

    def _load(self):
        databases = self._metabase.get_databases()

        self._entries = {}
        for db in databases:
            if db.get('is_saved_questions', False):
                for card in db['tables']:
                    question = card['id'].split('__')[-1]
                    question_name = f"questions.{question}"
                    description = card['display_name'] if card['description'] is None else card['description']
                    e = LocalCatalogEntry(
                        name=question_name,
                        description=description,
                        driver=MetabaseQuestionSource,
                        catalog=self,
                        args={
                            'domain': self.domain,
                            'username': self.username,
                            'password': self.password,
                            'token': self.token,
                            'question': question
                        }
                    )
                    e._plugin = [MetabaseQuestionSource]
                    self._entries[question_name] = e
            else:
                for table in db['tables']:
                    table_name = f"{db['name']}.{table['name']}"
                    e = LocalCatalogEntry(
                        name=table_name,
                        description=table['description'],
                        driver=MetabaseTableSource,
                        catalog=self,
                        args={
                            'domain': self.domain,
                            'username': self.username,
                            'password': self.password,
                            'token': self.token,
                            'database': db['id'],
                            'table': table['id']
                        }
                    )
                    e._plugin = [MetabaseTableSource]
                    self._entries[table_name] = e


class MetabaseQuestionSource(DataSource):
    name = 'metabase_question'
    container = 'dataframe'
    version = __version__
    partition_access = True

    def __init__(self, domain, question, username=None, password=None, token=None, metadata=None):
        self.domain = domain
        self.username = username
        self.password = password
        self.token = token
        self.question = question
        self._df = None

        self._metabase = MetabaseAPI(self.domain, self.username, self.password, self.token)

        super(MetabaseQuestionSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            self._df = self._metabase.get_card(self.question)

        return Schema(datashape=None,
                      dtype=self._df.dtypes,
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


class MetabaseTableSource(DataSource):
    name = 'metabase_table'
    container = 'dataframe'
    version = __version__
    partition_access = True

    def __init__(self, domain, database, table=None, query=None, username=None, password=None, token=None, metadata=None):
        self.domain = domain
        self.username = username
        self.password = password
        self.database = database
        self.token = token
        self.table = table
        self.query = query
        self._df = None

        self._metabase = MetabaseAPI(self.domain, self.username, self.password, self.token)

        super(MetabaseTableSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            self._df = self._metabase.get_table(self.database, self.table, self.query)

        return Schema(datashape=None,
                      dtype=self._df.dtypes,
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
    def __init__(self, domain, username=None, password=None, token=None):
        self.domain = domain

        self.password = password
        self.username = username

        if token is not None:
            self._token = token
            self._token_expiration = None
        else:
            self._token = None
            self._token_expiration = datetime.now()

    def _create_or_refresh_token(self):
        if self._token:
            if (self._token_expiration is None) or (datetime.now() < self._token_expiration):
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
        params = {'include': 'tables', 'saved': True}

        res = requests.get(
            urljoin(self.domain, '/api/database'),
            headers=headers, params=params
        )

        return res.json()['data']

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

    def get_card(self, question):
        from io import StringIO

        import pandas as pd

        self._create_or_refresh_token()

        card_metadata = self.get_metadata(f'card__{question}')
        date_fields = [f['display_name'] for f in card_metadata['fields']
                       if 'date' in f['base_type'].lower()]

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Metabase-Session': self._token
        }

        res = requests.post(
            urljoin(self.domain, f'/api/card/{question}/query/csv'),
            headers=headers
        )

        res.raise_for_status()
        csv = res.text

        return pd.read_csv(StringIO(csv.encode(res.encoding).decode('utf-8')),
                           parse_dates=date_fields, infer_datetime_format=True)

    def get_table(self, database, table=None, query=None):
        from io import StringIO

        import pandas as pd

        self._create_or_refresh_token()

        if (table is not None) and (query is not None):
            raise ValueError('Please set only one of table or query')

        kwargs = {}
        if table is not None:
            table_metadata = self.get_metadata(table)
            key = 'name' if query is not None else 'display_name'
            date_fields = [f[key] for f in table_metadata['fields']
                           if 'date' in f['base_type'].lower()]
            kwargs = {
                'parse_dates': date_fields,
                'infer_datetime_format': True
            }

        body = {
            "database": database,
        }

        if query is None:
            body['type'] = 'query'
            body['query'] = {'source-table': table}
        else:
            body['type'] = 'native'
            body['native'] = {'query': query}

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

        return pd.read_csv(StringIO(csv.encode(res.encoding).decode('utf-8')),
                           **kwargs)
