#!env python3
import pathlib
import tarfile
from io import TextIOWrapper
import time

import typesense
import typesense.document
import typesense.exceptions
import csv
import re

import zstandard
from unidecode import unidecode



# get latest dataset from https://data.metabrainz.org/pub/musicbrainz/canonical_data/

DATA_FILE_NAME = "musicbrainz-canonical-dump-20241103-080003.tar.zst"
DATA_FILE = pathlib.Path(DATA_FILE_NAME).resolve(strict=True)

TYPESENSE_COLLECTION = "musicbrainz"
TYPESENSE_HOST = "musicbrainz.int.liesdonk.nl"  # For Typesense Cloud use xxx.a1.typesense.net
TYPESENSE_PORT = 8108                           # For Typesense Cloud use 443
TYPESENSE_PROTOCOL = 'http'                     # For Typesense Cloud use https
TYPESENSE_API_KEY = 'lAUhlEkTApNcbVfgjX2wcRfhSQ0pNfEt' # Default 'xyz'

# Create connection client
client = typesense.Client({
    'nodes': [{
        'host': TYPESENSE_HOST,
        'port': TYPESENSE_PORT,
        'protocol': TYPESENSE_PROTOCOL
    }],
    'api_key': TYPESENSE_API_KEY,
    'connection_timeout_seconds': 300
})

# Define schema
musicbrainz_schema = {
    'name': TYPESENSE_COLLECTION,
    'fields': [
        {'name': 'artist_credit_name',
         'type': 'string'
         },
        {'name': 'release_name',
         'type': 'string'
         },
        {
            'name': 'recording_name',
            'type': 'string'
        },
        {
            'name': 'combined',
            'type': 'string'
        },
        {
            'name': 'score',
            'type': 'int32'
        },
    ],
    'default_sorting_field': 'score'
}


def import_data():
    # import from archive
    zstd_file = zstandard.open(DATA_FILE, mode='rb')
    tar_file = tarfile.open(fileobj=zstd_file, mode='r:')

    while (member := tar_file.next()) is not None:
        if not member.isfile():
            continue

        fo = tar_file.extractfile(member)
        filename = member.name.rsplit('/')[-1]
        match filename:
            case "TIMESTAMP":
                pass
            case "COPYING":
                # _logger.debug(fo.read().decode())
                pass
            case "canonical_musicbrainz_data.csv":
                with TextIOWrapper(fo, encoding='utf-8') as tw:
                    try:
                        documents = []
                        for i, row in enumerate(csv.reader(tw, delimiter=',')):
                            if i == 0:
                                continue

                            document = {
                                'artist_credit_id': row[1],
                                'artist_mbids': row[2],
                                'artist_credit_name': row[3],
                                'release_mbid': row[4],
                                'release_name': row[5],
                                'recording_mbid': row[6],
                                'recording_name': row[7],
                                'combined': row[8],
                                'score': int(row[9])
                            }
                            documents.append(document)

                            if len(documents) == 50000:
                                res = client.collections[TYPESENSE_COLLECTION].documents.import_(documents)
                                documents = []
                                print(f"imported {i:,} rows")
                                time.sleep(10)

                        if documents:
                            client.collections[TYPESENSE_COLLECTION].documents.import_(documents)

                    except typesense.exceptions.TypesenseClientError as err:
                        print("typesense index: Cannot build index: ", str(err))

            case "canonical_recording_redirect.csv":
                pass
            case "canonical_release_redirect.csv":
                pass
            case _:
                print(f"Don't know how to handle {filename}")
                break


def make_combined_lookup(artist_name, recording_name):
    """ Given the artist name and recording name, return a combined_lookup string """
    return unidecode(re.sub(r'[^\w]+', '', artist_name + recording_name).lower())


def search(artist_name, recording_name):
    query = make_combined_lookup(artist_name, recording_name)
    search_parameters = {'q': query, 'query_by': "combined", 'prefix': 'no', 'num_typos': 5}
    print(search_parameters)
    results = client.collections[TYPESENSE_COLLECTION].documents.search(search_parameters)
    return results


# Delete old collection
try:
    client.collections[TYPESENSE_COLLECTION].delete()
except typesense.exceptions.ObjectNotFound:
    pass


# Create collection
client.collections.create(musicbrainz_schema)

# Import data
import_data()

print(search("[Disney]", "King Scar"))

doc = client.collections[TYPESENSE_COLLECTION].documents[1].retrieve()

print(doc)
