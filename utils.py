import logging
import os


def configure_logging():
    log = logging.getLogger('warns')
    log.setLevel(level='WARNING')

    file_path = os.path.join(os.path.dirname(__file__), 'warns.log')
    file_handler = logging.FileHandler(
        filename=file_path,
        mode='a',
        encoding='utf8',
    )
    file_handler.setFormatter(logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M'
    ))
    file_handler.setLevel(level='WARNING')
    log.addHandler(file_handler)

    return log
