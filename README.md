# intake_metabase

Intake driver to load [Metabase tables](https://www.metabase.com/)

## Install

To install using Conda run

```
conda install -c defusco intake-metabase
```


## Quickstart
To access a catalog of tables in Metabase you will need the following information

* `domain`: The URL where Metabase is running
* `username`: Your username, typically an email address
* `password`: Your password (Google Auth is not yet supported)

To load the catalog and list the tables

```python
import intake
catalog = intake.open_metabase_catalog(domain, username, password)
list(catalog)
```

This will produce output like

```
['first-db.table_a', 'first-db.table_b', 'questions.3']
```

To load a table as a Pandas DataFrame

```
df = catalog['<table>'].read()    
```

Replace `<table>` with the name of the table from the list.

This driver supports multiple databases and saved questions.

## Load a single table
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
## Load a single question

To load a table as a Pandas DataFrames you will need to know the following information

* `domain`: The URL where Metabase is running
* `username`: Your username, typically an email address
* `password`: Your password (Google Auth is not yet supported)
* `question`: The numeric id of the question

You can generally determine the numeric id of the question you are interested in by

1. Visit `<domain>/collection/root?type=card`
1. Click on the question
    * You'll see in the url the question id `<domain>/question/<question_id>`

```python
import intake
ds = intake.open_metabase_question(domain, username, password,
                                   question)
df = ds.read()
```


## Constructing catalogs
This repository provides three drivers

* `metabase_catalog`: Catalog entry to retrieve all tables and questions
* `metabase_table`: Catalog entry for a single table in a database
* `metabase_question`: Catalog entry for a single saved question

To build a catalog containing a Metabase table it can be useful to use the 
[Catalog Templating](https://intake.readthedocs.io/en/latest/catalog.html#templating) features
to avoid writing usernames and passwords into the catalog. For example this catalog
provides a single table.

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
