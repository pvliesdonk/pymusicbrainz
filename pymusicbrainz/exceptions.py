class MBApiError(Exception):
    pass


class MBIDNotExistsError(MBApiError):
    pass


class NotFoundError(MBApiError):
    pass


class IllegalArgumentError(MBApiError):
    pass


class IllegaleRecordingReleaseGroupCombination(MBApiError):
    pass


class FactoryNotAvailable(MBApiError):
    pass


class TypesenseNotAvailable(MBApiError):
    pass


class CanonicalDBNotAvailable(MBApiError):
    pass


class NoCanonicalFound(MBApiError):
    pass
