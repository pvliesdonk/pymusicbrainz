class MBApiError(Exception):
    pass

class MBIDNotExistsError(MBApiError):
    pass

class NotFoundError(MBApiError):
    pass

class IllegalArgumentError(MBApiError):
    pass