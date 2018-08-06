"""Utilities related to logging."""
import logging
from typing import Optional


def add_logger(name: str=__name__,
               logfile: Optional[str]=None,
               fmt: str="%(asctime)s %(name)s %(levelname)s %(message)s",
               level: int=logging.INFO) -> logging.Logger:
    """Add a logger.

    Parameters
    ----------
    name
        Name of the logger.
    logfile
        A file to be used for logging. Possible values are None (do not log
        anything), "stdout" (print to STDOUT) or "<any string>" which will
        create a log file with the argument's name. Defaults to None.
    fmt
        Format string for the 'logging.Formatter'. Defaults to
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level:
        Minimum logging level to be logged. Defaults to 'logging.INFO' or 10.

    Returns
    -------
        logging.Logger: A logging object.
    """
    formatter = logging.Formatter(fmt=fmt)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logfile is None:
        handler = logging.NullHandler()  # type: logging.Handler
    elif logfile == "stdout":
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(logfile, mode="w")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def mute_dependency_loggers() -> None:
    """Mutes various dependency loggers."""
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    return None
