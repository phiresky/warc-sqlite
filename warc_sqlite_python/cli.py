import logging
import os
from pathlib import Path
import sys
from typing import Literal, Optional
from tap import Tap

from warc_sqlite_python import WebArchiveSqlite


class CommonArgs(Tap):
    db: Path
    """database file"""


class ImportArgs(CommonArgs):
    input: list[Path]
    """list of input paths. directory are recursively searched"""
    allow_existing: bool = True
    """
    if true the import command is idempotent, if false throw an error if an entry already exists
    """


class QueryArgs(CommonArgs):
    get_urls_like: Optional[str] = None
    get_url_payload: Optional[str] = None


def run_import():
    args = ImportArgs().parse_args()
    w = WebArchiveSqlite(args.db, write=True)
    w.add_warcs_from_dirs_or_files(args.input)


def run_query():
    args = QueryArgs().parse_args()
    w = WebArchiveSqlite(args.db, write=False)
    if args.get_url_payload:
        resp = w.get_by_url(args.get_url_payload, get_limit=1, only_ok=False)
        if len(resp) == 0:
            logging.error("Could not find response")
            os.exit(1)
        sys.stdout.buffer.write(resp[0].payload_data)
    if args.get_urls_like:
        # hacky glob conversion
        print("\n".join(w.get_urls_like(args.get_urls_like.replace("*", "%"))))
