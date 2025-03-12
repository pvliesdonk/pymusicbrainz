from abc import ABC, abstractproperty, abstractmethod

from .identifiers import MBID


class MusicBrainzObject(ABC):
    """Abstract object representing any of the primary Musicbrainz entities"""

    @property
    @abstractmethod
    def id(self) -> MBID:
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__str__()})"

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/artist/{self.id}"
