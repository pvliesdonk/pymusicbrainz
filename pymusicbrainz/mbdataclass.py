from abc import ABC
from dataclasses import dataclass


@dataclass
class MBDataObject(ABC):
    pass


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
