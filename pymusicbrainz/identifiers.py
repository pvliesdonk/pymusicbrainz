from __future__ import annotations

import logging
import uuid


class MBID:
    """Abstract representation of a Musicbrainz Identifier"""

    _logger = logging.getLogger(__name__)

    def __init__(self, mbid: str | uuid.UUID | MBID):

        if isinstance(mbid, str):
            self.mbid: uuid.UUID = uuid.UUID(mbid)

        elif isinstance(mbid, uuid.UUID):
            self.mbid = mbid

        elif isinstance(mbid, MBID):
            self.mbid = mbid.mbid

        else:
            self._logger.error(
                f"Trying to instantiate an object of type {type(mbid)}, which is not a string or uuid.UUID")

    def __repr__(self):
        return f"{type(self).__name__}({str(self.mbid)})"

    def __str__(self):
        return str(self.mbid)

    def __hash__(self):
        return hash(self.mbid.hex)

    def __eq__(self, other):
        return self.mbid == other.mbid

    @property
    def hex(self) -> str:
        return self.mbid.hex

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        return str(self.mbid).encode(encoding, errors)


class ArtistID(MBID):
    """Musicbrainz Artist ID"""

class ReleaseGroupID(MBID):
    """Musicbrainz Release Group ID"""


class ReleaseID(MBID):
    """Musicbrainz Release ID"""

class RecordingID(MBID):
    """Musicbrainz Recording ID"""


class MediumID(MBID):
    """Musicbrainz Medium ID"""

class TrackID(MBID):
    """Musicbrainz Track ID"""

class WorkID(MBID):
    """Musicbrainz Work ID"""
