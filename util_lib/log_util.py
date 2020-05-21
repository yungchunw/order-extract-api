import logging
import functools
import time

URL_LOGS = "./debug.log"
#Create two logger files    
func_fmt = logging.Formatter('%(asctime)s - %(filename)-20s - %(funcName)-15s - %(lineno)-3s -  %(levelname)-8s - %(message)s')
gen_fmt  = logging.Formatter('%(asctime)s - %(filename)-20s - %(funcName)-15s - %(lineno)-3s -  %(levelname)-8s - %(parse_id)s - %(pdfname)s - %(message)s')

# FUNC_LOG logger
func_logger = logging.getLogger('FUNC_LOG')
hdlr_1 = logging.FileHandler(URL_LOGS)
hdlr_1.setFormatter(func_fmt)
func_logger.setLevel(logging.DEBUG)
func_logger.addHandler(hdlr_1)

#GENERAL Logger
logger = logging.getLogger("GENERAL")
hdlr_2 = logging.FileHandler(URL_LOGS)
hdlr_2.setFormatter(gen_fmt)
logger.setLevel(logging.DEBUG)
logger.addHandler(hdlr_2)


def debug(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        func_logger.debug("[START] Entering Function [{:s}]...".format(fn.__name__))
        start_time = time.time()
        result = fn(*args, **kwargs)
        end_time = time.time() - start_time
        func_logger.debug("[END] Finished Function [{:s}], cost {:.3f} secs.".format(fn.__name__, end_time))
        return result

    return wrapper