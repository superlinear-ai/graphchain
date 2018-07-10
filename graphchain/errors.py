"""
Module containing basic exceptions used trhoughout the
`graphchain.py` and `funcutils.py` modules.
"""


class GraphchainCompressionMismatch(EnvironmentError):
    """
    Simple exception that is raised whenever the compression
    option in the `optimize` function does not match the one
    present in the `graphchain.json` file if such file exists.
    """
    pass
