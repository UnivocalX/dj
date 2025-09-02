class DatasetExist(Exception):
    pass


class DatasetNotFound(Exception):
    pass


class FileRecordNotFound(Exception):
    pass


class FileRecordExist(Exception):
    pass


class TagNotFound(Exception):
    pass


class FailedToGatherFiles(Exception):
    pass


class UnsuffiecentPermissions(Exception):
    pass