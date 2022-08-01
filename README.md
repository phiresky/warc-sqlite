# warc-sqlite

Simple demonstration of converting a set of warc.gz files into an indexed SQLite database. For context, see this discussion about WACZ: https://github.com/webrecorder/specs/issues/62

Notes:

- Data is stored by payload hash, so never stored twice
- Headers are stored as one long string - this should be improved since it's redundant and not indexable
- The only index there is right now allows querying by URL (exact or prefix)
    - for real use this would need to be expanded to search by other things such as request method, domain, ...
- In theory the goal is for format conversion should be lossless - so it should be possible to convert the SQLite file back to WARC

## Usage

To install:

```sh
pip install git+https://github.com/phiresky/warc-sqlite
```


To convert a directory containing warc.gz files (recursively):

```sh
warc-sqlite-import --db mydb.sqlite3 --input browsertrix/crawls/mycrawl
```

To get a list of urls (by a glob)

```sh
$ warc-sqlite-query --db mydb.sqlite3 --get_urls_like 'http://*example.com*'
http://example.com/
https://www.iana.org/domains/reserved
...
```

Get the raw payload of a url response

```sh
$ warc-sqlite-query --db mydb.sqlite3 --get_url_payload 'https://example.com'
<!doctype html>
<html>
<head>
    <title>Example Domain</title>

    <meta charset="utf-8" />
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    ...
```


### doc:

```
usage: warc-sqlite-import --input [INPUT ...] [--allow_existing] --db DB [-h]

options:
  --input [INPUT ...]  (list[pathlib.Path], required) list of input paths. directory are
                       recursively searched
  --allow_existing     (bool, default=True) if true the import command is idempotent, if
                       false throw an error if an entry already exists
  --db DB              (Path, required) database file
  -h, --help           show this help message and exit
```

```
usage: warc-sqlite-query [--get_urls_like GET_URLS_LIKE] [--get_url_payload GET_URL_PAYLOAD] --db DB [-h]

options:
  --get_urls_like GET_URLS_LIKE
                        (Optional[str], default=None) print out a list of urls that match the given glob
  --get_url_payload GET_URL_PAYLOAD
                        (Optional[str], default=None) print the raw payload of the first response to an url to stdout
  --db DB               (Path, required) database file
  -h, --help            show this help message and exit
```
