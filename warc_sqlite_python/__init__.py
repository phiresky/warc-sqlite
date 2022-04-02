from __future__ import annotations
import logging
import sqlite3
from pathlib import Path
from typing import Any, Union
from typing import NamedTuple

import warcio  # type: ignore
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper  # type: ignore


def insert_str(d: dict[str, Any]) -> str:
    keys1 = ", ".join(d.keys())
    keys2 = ", ".join([f":{key}" for key in d.keys()])
    return f"({keys1}) values ({keys2})"


class UrlResponseData(NamedTuple):
    url: str
    warc_record_id: str
    warc_headers: str
    http_headers: str
    http_status: int
    warc_type: str
    payload_hash: str
    payload_data: bytes


def open_sqlite_db(
    fname: Path, *, write: bool, isolation_level: Union[str, None] = None
) -> sqlite3.Connection:
    """
    connects to a sqlite db, with some defaults for performance
    WARNING: by default uses auto-commit (isolation_level=None), which is more sane than the python default of DEFERRED
    """

    try:
        timeout = 10
        mode = "rwc" if write else "ro"
        db = sqlite3.connect(
            f"file:{fname}?mode={mode}",
            uri=True,
            timeout=timeout,
            isolation_level=isolation_level,
        )
    except sqlite3.OperationalError as sqlite_err:
        logging.error(f"sqlite db {fname=} {timeout=}, {isolation_level=}")
        raise sqlite_err

    db.row_factory = (
        sqlite3.Row
    )  # allow accessing result columns by name. without this, you get ugly tuples

    db.execute("pragma foreign_keys = ON;")
    # unnecessary stuff for 𝐏𝐄𝐀𝐊 𝐏𝐄𝐑𝐅𝐎𝐑𝐌𝐀𝐍𝐂𝐄
    db.execute("pragma page_size = 32768;")
    db.execute("pragma temp_store = memory;")
    db.execute(f"pragma mmap_size={30 * 1000 * 1e6};")
    db.execute("pragma cache_size=-30000")
    if write:
        db.execute("pragma auto_vacuum = incremental;")
        db.execute("pragma incremental_vacuum;")
        db.execute("pragma journal_mode = WAL;")
        db.execute("pragma synchronous = off;")
        db.execute("pragma optimize;")
    return db


class WebArchiveSqlite:
    """
    a scraped web archive, converted from WARC format to sqlite for easier access
    """

    def __init__(self, db_path: Path, write: bool):
        self.db_path = db_path
        self.db = open_sqlite_db(self.db_path, write=write)
        if write:
            self.db.executescript(
                """
                create table if not exists warc_records (
                    warc_record_id text not null primary key,
                    warc_type text not null,
                    warc_headers text not null,
                    http_headers text,
                    payload_hash text references payloads(hash)
                ) strict; -- with rowid

                create table if not exists payloads (
                    hash text primary key not null,
                    data blob not null
                ) strict;

                create table if not exists responses_index (
                    url text not null,
                    response_id text not null references warc_records(warc_record_id),
                    http_status int not null,
                    payload_hash text references payloads(hash)
                ) strict;
                
                create index if not exists responses_index_by_url on responses_index(url);
            """
            )

    def add_warcs_from_dirs_or_files(
        self, inputs: list[Path], skip_existing: bool = True
    ) -> None:
        warc_files = []
        for input in inputs:
            if input.is_file():
                warc_files.add(input)
            else:
                warc_files.extend(input.glob("**/*.warc"))
                warc_files.extend(input.glob("**/*.warc.gz"))

        total_bytes = sum(file.stat().st_size for file in warc_files)
        progress: Any = tqdm(total=total_bytes, desc="warcs", unit="B", unit_scale=True)
        for file in warc_files:
            self.add_warc_file(file, skip_existing=skip_existing)
            progress.update(file.stat().st_size)
        logging.warning("vacuuming...")
        self.db.execute("vacuum")

    def add_warc_file(self, path: Path, skip_existing: bool) -> None:
        with Path(path).open("rb") as f:
            with tqdm(  # type: ignore
                total=path.stat().st_size, unit="B", unit_scale=True, desc=path.name
            ) as t:
                f = CallbackIOWrapper(t.update, f, "read")
                it = warcio.ArchiveIterator(f)
                for entry in it:
                    self.add_warc_record(entry, skip_existing)

    def add_warc_record(self, entry: warcio.ArcWarcRecord, skip_existing: bool):
        warc_record_id = entry.rec_headers.get_header("WARC-Record-ID")
        if skip_existing:
            has_already = self.db.execute(
                "select count(*) from warc_records where warc_record_id = ?",
                [warc_record_id],
            ).fetchone()
            if has_already[0] == 1:
                return
        payload_hash = entry.rec_headers.get_header("WARC-Payload-Digest")

        payload_data = dict(hash=payload_hash, data=entry.content_stream().read())
        self.db.execute(
            f"insert into payloads {insert_str(payload_data)} on conflict (hash) do nothing",
            payload_data,
        )
        entry_data = dict(
            warc_record_id=warc_record_id,
            warc_type=entry.rec_type,
            warc_headers=str(entry.rec_headers),
            http_headers=str(entry.http_headers),
            payload_hash=payload_hash,
        )
        upsert_str = "on conflict (warc_record_id) do nothing" if skip_existing else ""
        self.db.execute(
            f"insert into warc_records {insert_str(entry_data)} {upsert_str}",
            entry_data,
        )
        self._insert_into_indexes(entry)

    def _insert_into_indexes(self, entry: warcio.ArcWarcRecord):
        warc_record_id = entry.rec_headers.get_header("WARC-Record-ID")
        target_url = entry.rec_headers.get_header("WARC-Target-URI")
        payload_hash = entry.rec_headers.get_header("WARC-Payload-Digest")
        if entry.rec_type == "response":
            response_data = dict(
                url=target_url,
                http_status=entry.http_headers.get_statuscode(),
                response_id=warc_record_id,
                payload_hash=payload_hash,
            )
            self.db.execute(
                f"insert into responses_index {insert_str(response_data)}",
                response_data,
            )

    def get_urls_like(self, url_like: str) -> list[str]:
        rows = self.db.execute(
            "select url from responses_index where url like ?", [url_like]
        ).fetchall()
        return [row[0] for row in rows]

    def get_by_url(
        self, url: str, get_limit: int, only_ok: bool
    ) -> list[UrlResponseData]:
        only_ok_str = "and responses_index.http_status < 300" if only_ok else ""
        results = self.db.execute(
            f"""
            select
                url,
                warc_record_id,
                warc_headers,
                http_headers,
                http_status,
                warc_type,
                responses_index.payload_hash as payload_hash,
                payloads.data as payload_data
            from responses_index
            join warc_records on response_id = warc_record_id
            join payloads on responses_index.payload_hash = payloads.hash
            where url = ?
            {only_ok_str}
            limit ?
            """,
            [url, get_limit],
        ).fetchall()
        return [UrlResponseData(**result) for result in results]
