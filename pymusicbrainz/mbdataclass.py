from abc import ABC
from dataclasses import dataclass

from . import identifiers, factory


@dataclass
class MBDataObject(ABC):

    id: identifiers.MBID
    factory: factory.MBFactory

    @property
    def type(self) -> str:
        return self.__class__.__name__.lower()



@dataclass
class Artist(MBDataObject):
    pass


class ReleaseGroup(MBDataObject):
    pass


class Release(MBDataObject):
    pass


class Recording(MBDataObject):
    pass


class Medium(MBDataObject):
    pass


class Track(MBDataObject):
    pass


class Work(MBDataObject):
    pass
