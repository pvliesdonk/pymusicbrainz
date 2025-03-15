from __future__ import annotations

import logging
import uuid


class MBID:
    """Abstract representation of a Musicbrainz Identifier"""

    _logger = logging.getLogger(__name__)

    def __init__(self, mbid: str | uuid.UUID):

        if isinstance(mbid, str):
            self.mbid: uuid.UUID = uuid.UUID(mbid)

        elif isinstance(mbid, uuid.UUID):
            self.mbid = mbid

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

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)

class ReleaseGroupID(MBID):
    """Musicbrainz Release Group ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)


class ReleaseID(MBID):
    """Musicbrainz Release ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)


class RecordingID(MBID):
    """Musicbrainz Recording ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)


class MediumID(MBID):
    """Musicbrainz Medium ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)


class TrackID(MBID):
    """Musicbrainz Track ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)


class WorkID(MBID):
    """Musicbrainz Work ID"""

    def __init__(self, mbid: str | uuid.UUID):
        super().__init__(mbid)
