################################
# These exceptions represent internal errors
################################


class InternalException(Exception):
    pass


class InvalidConfigurationException(InternalException):
    pass


################################
# These exceptions represent query-time errors
################################


class InvalidRequestException(Exception):
    pass


class StateNotFoundException(InvalidRequestException):
    pass


class dbtCoreCompilationException(InvalidRequestException):
    pass


class UnsupportedQueryException(InvalidRequestException):
    pass
