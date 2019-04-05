import json
import pickle
from datetime import datetime
import logging
from utils import get_pickle_data, save_pickle_data, save_json, load_json, scanfiles, scan_packagefiles

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

base_name = "kt_api_data_AUD"
pickle_file = "{}.pickle".format(base_name)
logging.info("using pickle: {}".format(pickle_file))
data = get_pickle_data(pickle_file)

logger.info("Data keys loaded...")
for key, value in data.items():
    if value:
        logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
        if key in ['packages', 'pricelist']:
            max_len = 4
            i = 0
            if isinstance(value, list):
                for item in value:
                    i += 1
                    logger.info("\t\t{} => {} : {}".format(i, type(item), len(item)))
                    if i > max_len:
                        break
            elif isinstance(value, dict):
                for k, v in value.items():
                    logger.info("\t\t{} => {}".format(k, len(v)))
                    i += 1
                    if i > max_len:
                        break
    else:
        logger.info("\t{} : No Values".format(key))

save_json("{}.json".format(base_name), data)