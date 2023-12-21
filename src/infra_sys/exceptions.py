"""Defines all exceptions in the package."""


class ISBaseException(Exception):
    """Base class for all exceptions in the package"""


class ISAlreadyAttached(ISBaseException):
    """Raised if the component is already attached to a system."""


class ISDuplicateNames(ISBaseException):
    """Raised if the components with duplicate type and name are stored."""


class ISFileExists(ISBaseException):
    """Raised if the file already exists."""


class ISConflictingArguments(ISBaseException):
    """Raised if the arguments are conflict."""


class ISConflictingSystem(ISBaseException):
    """Raised if the system has conflicting values."""


class ISNotStored(ISBaseException):
    """Raised if the requested object is not stored."""


class ISOperationNotAllowed(ISBaseException):
    """Raised if the requested operation is not allowed."""
