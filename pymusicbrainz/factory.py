from abc import ABC


class MBFactory(ABC):
    pass


class DBFactory(MBFactory):
    pass


class APIFactory(MBFactory):
    pass
