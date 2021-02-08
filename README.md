# intake_metabase

Intake driver to load [Metabase tables](https://www.metabase.com/)

## Install

To install using Conda run

```
conda install -c defusco intake-metabase
```


## Quickstart

To load a table as a Pandas DataFrames you will need to know the following information

* `domain`: The URL where Metabase is running
* `username`: Your username, typically an email address
* `password`: Your password (Google Auth is not yet supported)
* `database`: The numeric id of the database where the table is stored
* `table`: The numeric id of the table to load

You can generally determine the numeric ids of the database and table from the URL when you are viewing the table in your browser. Here are the steps.

1. Visit `<domain>/reference`
1. Click on the database you want
    1. You'll now see that the url has changed to `<domain>/reference/databases/<database>` where `database` is the numeric id of the database.
1. Click on `Tables in <database-name>`
1. Click on your desired table
    1. You'll now see that the url has changed to `<domain>/reference/databases/<database>/tables/<table>` where `table` is the numeric id of the table.

Once you have all of the above information you can load a table as follows

```python
import intake
ds = intake.open_metabase_table(domain, username, password,
                                database, table)
df = ds.read()
```

## Constructing catalogs

To build a catalog containing a Metabase table it can be useful to use the 
[Catalog Templating](https://intake.readthedocs.io/en/latest/catalog.html#templating) features
to avoid writing usernames and passwords into the catalog

```yaml
metadata:
  version: 1

sources:
  my_table:
    description: My table
    driver: metabase_table
    args:
      domain: <domain>
      username: 'env("METABASE_USERNAME")'
      password: 'env("METABASE_PASSWORD")'
      database: 2
      table: 6
```
