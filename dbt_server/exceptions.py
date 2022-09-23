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
    @classmethod
    def from_exc(cls, e):
        original_message = getattr(e, 'msg', str(e))
        msg = f"dbt Error: {original_message}"
        return cls(msg)
