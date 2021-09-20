import logging
import functools
from .config import log_file, log_level
import os



def _generate_log(path=log_file):
    """
    Create a logger object.
    :param path: Path of the log file.
    :return: Logger object.
    """
    # Create a logger and set the level.
    logger = logging.getLogger('__name__')

    path = os.path.expanduser(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Create file handler, log format and add the format to file handler
    file_handler = logging.FileHandler(path)

    # See https://docs.python.org/3/library/logging.html#logrecord-attributes
    # for log format attributes.
    log_format = '%(levelname)s %(asctime)s %(message)s'
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)

    logging.basicConfig(level=log_level, format='%(levelname)s %(asctime)s %(message)s')

    logger.addHandler(file_handler)
    return logger

def pretty_df(object_to_pprint):
    """
    A wrapper to handle some more acceptable printing of dfs to the log
    :param object_to_pprint: Any object that will be passed to the logger
    :return: The linearized head(3) + shape of the df/Series if Pandas, repr if available, empty string otherwise
    """
    try:
        ttab = f'Pandas object : {object_to_pprint.head(3).to_string(index=False)}  of shape : {object_to_pprint.shape}'
    except Exception:
        try:
            ttab = repr(object_to_pprint)
        except Exception:
            ttab = ''
    return ttab


def log_ingester(path=log_file):
    """
    A decorator wrapper for the ingester.
    :param path: A logfile; otherwise will use the logfile in the config system
    :return: Decorator
    """
    if path is None:
        path = '/dev/null'

    def log(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):


            logger = _generate_log(path)

            try:
                args_passed_in_function = [pretty_df(a) for a in args]
                kwargs_passed_in_function = [f"{k}={pretty_df(v)!r}" for k, v in kwargs.items()]
                args_kwargs_passed_in_function = args_passed_in_function + kwargs_passed_in_function
                logger.debug(f'Call: {func.__name__}')
                formatted_arguments = 'Arguments passed:' + ', '.join(args_kwargs_passed_in_function)
                logger.debug(formatted_arguments)
                return func(*args, **kwargs)

            except Exception as e:
                error_msg = 'An error has occurred at /' + func.__name__ + '\n'
                logger.exception(error_msg)

                return e  # Or whatever message you want.

        return wrapper

    return log


