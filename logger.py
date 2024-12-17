"""
##############################Module Information###############################
#   Module name         :   logger
#   Purpose             :   Handles all logging creation functions
#   Input               :   NA
#   Output              :   NA
#   Execution steps     :   NA
#   Predecessor module  :   NA
#   Successor module    :   NA
#   Pre-requisites      :   NA
#   Last changed on     :   1st August 2024
#   Last changed by     :   Cameron Cederquist
#   Reason for change   :   Added extra comments
###############################################################################
"""

# libraries and external libraries definition
from logging import Formatter, StreamHandler, getLogger, DEBUG
from os import listdir, mkdir
from pathlib import Path
from sys import stdout
from queue import SimpleQueue
from logging.handlers import TimedRotatingFileHandler, QueueHandler, QueueListener

"""
RUNS AT FIRST COMPILE: makes sure a logging file exists
"""
# required line to put home folder in correct place
HOME_FOLDER = str(Path.home())
# define a logging format using a Formatter object
FORMATTER = Formatter("%(asctime)s : %(threadName)s : %(levelname)s : %(message)s")
# create logs folder if not already present
if 'logs' not in listdir():
    mkdir('logs')
# create log file if not already present
LOG_FILE = "logs/orchestrator_logs.log"
try:
    with open(LOG_FILE, "w"):
        pass
    print(f"Empty file '{LOG_FILE}' has been created.")
except IOError as e:
    print(f"Error creating file: {e}")


def get_console_handler():
    """
    Creates and returns a StreamHandler object that directs logs to the console (stdout)

    Returns
    --------------
    console_handler: StreamHandler
        handler for printing logs to stdout
    """
    console_handler = StreamHandler(stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    """
    Creates and returns a FileHandler that stores logs in a log file.
    uses TimedRotatingFileHandler, which wipes logs every midnight.

    Returns
    --------------
    file_handler: TimedRotatingFileHandler
        handler for storing logs in log file
    """
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight', backupCount=3)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def create_logger(logger_name, log_level):
    """
    Creates a Logger object attached to a QueueHandler

    Parameters
    --------------
    logger_name: str
        name id of Logger
    log_level: str
        Logger log level. see LEGAL_LOGGING_LEVELS in config_manager
    queue: SimpleQueue
        queue to be attached to Logger

    Returns
    --------------
    logger: Logger
        logging object
    """
    queue = SimpleQueue()
    logger = getLogger(logger_name)
    logger.setLevel(log_level)
    queue_handler = QueueHandler(queue)
    listener = QueueListener(queue, get_console_handler(), get_file_handler())
    logger.addHandler(queue_handler)
    logger.propagate = False
    return logger, listener
