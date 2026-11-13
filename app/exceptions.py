class PermanentJobError(Exception):
    pass


class RetryableJobError(Exception):
    pass


class JobConflictError(Exception):
    pass
