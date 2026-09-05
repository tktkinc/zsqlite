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
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        connection.close()

        anchor = database.read_bytes()
        assert len(anchor) == 4096 and anchor.startswith(b"ZSQLAN03")
        sidecar = Path(f"{database}-zsqlite")
        assert sidecar.is_file() and sidecar.stat().st_size > 12_288
        assert Path(f"{database}-zsqlite-lock").is_file()
        assert Path(f"{database}-zsqlite-publish").is_file()

        reopened = sqlite3.connect(uri, uri=True)
        assert reopened.execute("PRAGMA page_size").fetchone() == (8192,)
        assert reopened.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert reopened.execute(
            "SELECT count(*), sum(length(body)), sum(length(metadata)) FROM transcript"
        ).fetchone() == (300, 351_042, 33_842)
        reopened.close()

        if args.cli is not None:
            subprocess.run([args.cli, "verify", database], check=True)
            result = subprocess.run(
                [args.cli, "inspect", database],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            assert "page_size: 8192" in result.stdout
            assert "format: V3 native" in result.stdout

            exported = Path(directory) / "exported.db"
            subprocess.run([args.cli, "export", database, exported], check=True)
            native = sqlite3.connect(exported)
            assert native.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert native.execute("SELECT count(*) FROM transcript").fetchone() == (300,)
            native.close()


if __name__ == "__main__":
    main()
