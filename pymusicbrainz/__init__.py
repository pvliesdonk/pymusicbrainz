from __future__ import annotations

import pathlib

import urllib3

from pymusicbrainz import db, config
from pymusicbrainz.identifiers import *
from pymusicbrainz.factory import MBFactory
from pymusicbrainz.canonical import CanonicalSearch

from pymusicbrainz.search import Search


def get_factory(shelf_file: pathlib.Path = None) -> MBFactory:
    """Get the factory object for Musicbrainz objects

    :return: Factory object
    """
    return MBFactory.get_factory(shelf_file)


def configure_database(db_url: str = None, echo_sql: bool = False) -> None:
    """Configure the PostgreSQL database for Musicbrainz

    :param db_url: URI for PostgreSQL database
    :param echo_sql: Echo all SQL statements to stdout
    """
    db.configure_database(db_url=db_url, echo_sql=echo_sql)


def get_search() -> Search:
    """Get a searcher object

    :return: Search object"""
    return Search.get_instance()


def get_canonical_search(
    db_file: pathlib.Path = config.DEFAULT_CANONICAL_DB,
    url: urllib3.util.Url = config.DEFAULT_TYPESENSE_URL,
    api_key: str = config.DEFAULT_TYPESENSE_API_KEY,
    collection: str = config.DEFAULT_TYPESENSE_SEARCH_FIELD,
    search_field: str = config.DEFAULT_TYPESENSE_COLLECTION,
) -> CanonicalSearch:
    return CanonicalSearch(
        db_file=db_file,
        url=url,
        api_key=api_key,
        collection=collection,
        search_field=search_field,
    )
