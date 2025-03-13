from abc import ABC, abstractmethod

from pymusicbrainz.datatypes.identifiers import MBID


class MusicBrainzObject(ABC):
    """Abstract object representing any of the primary Musicbrainz entities"""

    @property
    @abstractmethod
    def id(self) -> MBID:
        pass

    @property
    @abstractmethod
    def type(self) -> str:
        pass

    @property
    @abstractmethod
    def backing(self) -> str:
        pass


    def __repr__(self):
        return f"{self.__class__.__name__}({self.__str__()})"

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/{self.type}/{self.id}"

