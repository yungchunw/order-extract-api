import logging
import functools
import time

logger = logging.getLogger()
fmt = '%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='./debug.log',level=logging.DEBUG, 
                    format=fmt)



def debug(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        logger.debug("[START] Entering Function [{:s}]...".format(fn.__name__))
        start_time = time.time()
        result = fn(*args, **kwargs)
        end_time = time.time() - start_time
        logger.debug("[END] Finished Function [{:s}], cost {:.3f} secs.".format(fn.__name__, end_time))
        return result

    return wrapper