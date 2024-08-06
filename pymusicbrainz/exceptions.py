class MBApiError(Exception):
    pass

class MBIDNotExistsError(MBApiError):
    pass

class NotFoundError(MBApiError):
    pass
