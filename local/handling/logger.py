import logging
import os
import sys
from datetime import datetime

'''
Logging module
'''
LOG_FILE = f"{datetime.now().strftime('%d-%m-%Y %H h %M m %S s')}.log"
# LOG_FILE = f"{datetime.now().isoformat()}.log"
logs_path = os.path.join(os.getcwd(),"logs")
os.makedirs(logs_path,exist_ok=True)

LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

logging.basicConfig(
    format="[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level = logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]

)


if __name__ == '__main__':
    logging.info("Logging is working")