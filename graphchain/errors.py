"""
Module containing basic exceptions used trhoughout the
`graphchain.py` and `funcutils.py` modules.
"""


class InvalidPersistencyOption(ValueError):
    """
    Simple exception that is raised whenever the persistency
    option in the `optimize` function does not match the one
    of the supported options "local" or "s3".
    """
    pass


class GraphchainCompressionMismatch(EnvironmentError):
    """
    Simple exception that is raised whenever the compression
    option in the `optimize` function does not match the one
    present in the `graphchain.json` file if such file exists.
    """
    pass


class GraphchainPicklingError(AttributeError):
    """
    Simple exception that is raised whenever a serialization
    operation fails.
    """
    pass
