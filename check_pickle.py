import json
import pickle
from datetime import datetime
import logging
from utils import get_pickle_data, save_pickle_data, save_json, load_json, scanfiles, scan_packagefiles

logger = logging.getLogger(__name__)

base_name = "kt_api_data_USD"
pickle_file = "{}.pickle".format(base_name)
data = get_pickle_data(pickle_file)

if logger.level == logging.INFO and len(data)> 0:
    logger.info("Data keys loaded...")
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
        else:
            logger.info("\t{} : No Values".format(key))
else:
    logger.info('Clean data file...')

save_json("{}.json".format(base_name), data)