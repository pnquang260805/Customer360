import logging
import os
import functools

try:
    os.mkdir("./log/")
except FileExistsError:
    pass


logging.basicConfig(
    level=logging.INFO,
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s(): %(message)s", datefmt="%Y-%m-%d %H:%M",)

__console_handler = logging.StreamHandler()
__file_handler = logging.FileHandler("./log/ingest-data.log", "a", "utf-8")
__file_handler.setFormatter(formatter)

log = logging.getLogger(__name__)  # type: ignore
log.addHandler(__console_handler)
log.addHandler(__file_handler)

def auto_log(message_prefix=""):
    def logger(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                log.error(e)
                raise(e)
                
        return wrapper
    return logger