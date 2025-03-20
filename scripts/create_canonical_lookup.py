#!env python3
from __future__ import annotations

import csv
import dbm
import pathlib
import shelve
import tarfile
from io import TextIOWrapper

import zstandard

# get latest dataset from https://data.metabrainz.org/pub/musicbrainz/canonical_data/

DATA_FILE_NAME = "musicbrainz-canonical-dump-20250317-080003.tar.zst"
DATA_FILE = pathlib.Path(DATA_FILE_NAME).resolve(strict=True)


# import from archive
zstd_file = zstandard.open(DATA_FILE, mode="rb")
tar_file = tarfile.open(fileobj=zstd_file, mode="r:")

db = dbm.open("../canonical_redirect", flag="c")


while (member := tar_file.next()) is not None:
    if not member.isfile():
        continue

    fo = tar_file.extractfile(member)
    filename = member.name.rsplit("/")[-1]
    match filename:
        case "TIMESTAMP":
            pass
        case "COPYING":
            # _logger.debug(fo.read().decode())
            pass
        case "canonical_musicbrainz_data.csv":
            pass

        case "canonical_recording_redirect.csv":
            with TextIOWrapper(fo, encoding="utf-8") as tw:
                for i, row in enumerate(csv.reader(tw, delimiter=",")):
                    if i == 0:
                        continue

                    if i % 1000 == 0:
                        print(f"{i} canonical recording redirects imported")
                    db[row[0]] = f"{row[1]};{row[2]}".encode()

        case "canonical_release_redirect.csv":
            with TextIOWrapper(fo, encoding="utf-8") as tw:
                for i, row in enumerate(csv.reader(tw, delimiter=",")):
                    if i % 1000 == 0:
                        print(f"{i} canonical release redirects imported")
                    db[row[0]] = f"{row[1]};{row[2]}".encode()

        case _:
            print(f"Don't know how to handle {filename}")
            break

db.close()
