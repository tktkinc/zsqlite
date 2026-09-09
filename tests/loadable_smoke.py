#!/usr/bin/env python3
"""End-to-end smoke test using a host SQLite library and the loadable extension."""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("extension", type=Path)
    parser.add_argument("--cli", type=Path)
    args = parser.parse_args()
    extension = args.extension.resolve()
    if not extension.is_file():
        raise FileNotFoundError(extension)

    with tempfile.TemporaryDirectory(prefix="zsqlite-loadable-") as directory:
        database = Path(directory) / "host.db"
        storage = Path(f"{database}.zsqlite")
        bootstrap = sqlite3.connect(":memory:")
        bootstrap.enable_load_extension(True)
        bootstrap.execute(
            "SELECT load_extension(?, 'sqlite3_zsqlite_init')", (str(extension),)
        )
        bootstrap.close()

        uri = f"file:{database.as_posix()}?vfs=zsqlite"
        connection = sqlite3.connect(uri, uri=True)
        assert connection.execute("PRAGMA page_size=8192").fetchone() is None
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute(
            "CREATE TABLE transcript(id INTEGER PRIMARY KEY, body TEXT, metadata BLOB)"
        )
        connection.executemany(
            "INSERT INTO transcript VALUES(?, ?, ?)",
            (
                (
                    index,
                    f'{{"id":{index},"body":"' + "x" * (1000 + index) + '"}',
                    bytes((index + offset) % 256 for offset in range(index % 257)),
                )
                for index in range(1, 301)
            ),
        )
        connection.commit()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("SELECT count(*) FROM transcript").fetchone() == (300,)
        assert Path(f"{storage}-wal").is_file()
        assert not Path(f"{database}-wal").exists()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        connection.close()

        notice = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
        assert notice.execute(
            "SELECT message FROM zsqlite_extension_required"
        ).fetchone() == (
            "This database uses zsqlite storage. Load the zsqlite extension and reopen with vfs=zsqlite.",
        )
        notice.close()

        active = storage.read_bytes()
        assert len(active) >= 4_096 and active.startswith(b"ZSQLSE06")
        assert database.read_bytes().startswith(b"SQLite format 3\x00")
        sidecar = Path(f"{storage}.d")
        assert sidecar.is_dir()
        assert not (sidecar / "roots").exists()
        assert not (sidecar / "active").exists()
        assert (sidecar / "locks" / "lifecycle.lock").is_file()
        assert (sidecar / "locks" / "publication.lock").is_file()
        assert (sidecar / "locks" / "sqlite.lock").is_file()

        reopened = sqlite3.connect(uri, uri=True)
        assert reopened.execute("PRAGMA page_size").fetchone() == (8192,)
        assert reopened.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert reopened.execute(
            "SELECT count(*), sum(length(body)), sum(length(metadata)) FROM transcript"
        ).fetchone() == (300, 351_042, 33_842)
        reopened.close()

        if args.cli is not None:
            subprocess.run([args.cli, "verify", database], check=True)
            subprocess.run([args.cli, "flush", database], check=True)
            assert any((sidecar / "segments").glob("*.zseg"))
            result = subprocess.run(
                [args.cli, "inspect", database],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            assert "page_size: 8192" in result.stdout
            assert "format: V6 active-segment" in result.stdout

            exported = Path(directory) / "exported.db"
            subprocess.run([args.cli, "export", database, exported], check=True)
            native = sqlite3.connect(exported)
            assert native.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert native.execute("SELECT count(*) FROM transcript").fetchone() == (300,)
            native.close()


if __name__ == "__main__":
    main()
