"""
Utilities related to logging.
"""
import logging


def init_logging(name=__name__,
                 logfile=None,
                 fmt="%(asctime)s - %(name)s - " +
                     "%(levelname)s - %(message)s",
                 level=logging.DEBUG):
    """
    Function that enables logging by returning a logger object
    named 'name' that can be used for logging purposes.

    Args:
        name (str): Name of the logger.
        logfile (str, optional): A file to be used for logging.
            Possible values are None (do not log anything),
            "stdout" (print to STDOUT) or "<any string>" which will
            create a log file with the argument's name.
            Defaults to None.
        fmt (str, optional): Format string for the 'logging.Formatter'.
            Defaults to '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        level (int, optional): Minimum logging level to be logged.
            Defaults to 'logging.DEBUG' or 10.
    Returns:
        logging.Logger: A logging object.
    """
    formatter = logging.Formatter(fmt)
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logfile is None:
        handler = logging.NullHandler()
    elif logfile == "stdout":
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(logfile, mode="w")
    handler.setFormatter(formatter)
    handler.setLevel(level)

    logger.addHandler(handler)
    return logger


def disable_deps_logging():
    """
    Disables various dependencies logging.
    """
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    return None
