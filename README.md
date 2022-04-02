# warc-sqlite

Simple demonstration of converting a set of warc.gz files into an indexed SQLite database.

Notes:

- Data is stored by payload hash, so never stored twice
- Headers are stored as one long string - this should be improved since it's redundant and not indexable
- The only index there is right now allows querying by URL (exact or prefix)
    - for real use this would need to be expanded to search by other things such as request method, domain, ...
- In theory the goal is for format conversion should be lossless - so it should be possible to convert the SQLite file back to WARC

## Usage

To install:

```sh
pip install git+https://github.com/phiresky/warc-sqlite-python
```


To convert a directory containing warc.gz files (recursively):

```sh
warc-sqlite-import --db mydb.sqlite3 --input browsertrix/crawls/mycrawl
```

To get a list of urls (by a glob)

```sh
warc-sqlite-query --db mydb.sqlite3 --get_urls_like 'https://*example.com*'
```

Get the raw payload of a url response

```sh
warc-sqlite-query --db mydb.sqlite3 --get_url_payload 'https://example.com'
```