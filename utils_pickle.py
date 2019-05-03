import json
import pickle
import os
from datetime import datetime
import logging
from utils import get_pickle_data, save_pickle_data, save_json, load_json, scanfiles, scan_packagefiles
from utils_config import get_folderpath, load_config, get_configuration_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_pickle(config, data, kt, savepath):
    pickle_file = config.get('presets', {}).get('pickle')
    if not pickle_file:
        return data

    logging.info("using pickle: {}".format(pickle_file))
    base_name, _ = os.path.splitext(pickle_file)

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
    return data
    
if __name__ == "__main__":
    PATHS = {
        'LOCAL': os.getcwd(),
        'HOME': os.path.expanduser("~") 
    }

    config = load_config('ktapi.json')
    try:
        logging.config.dictConfig(config.get('logger', {}))
    except:
        logging.basicConfig(level=logging.INFO)
    config["paths"] = PATHS

    savepath = get_folderpath(config, '_remote', PATHS)
    logger.info('Savepath: {}'.format(savepath))

    currencies = config.get("presets", {}).get("currencies", ["CAD"])
    for currency in currencies:
        config['presets']['currency'] = currency
        config_type = config.get("configurations", {}).get("run", {}).get("pickle")
        pickle_file = get_configuration_path(config, config_type, PATHS)
        name, ext = os.path.splitext(pickle_file)
        pickle_file = "{}_{}{}".format(name,currency, ext)
        logger.info("Loading pickle file {}".format(pickle_file))
        if not 'presets' in config:
            config['presets']

        config['presets']['pickle'] = pickle_file
        data = get_pickle_data(pickle_file)

        export_pickle(config=config, data=data, kt=None, savepath=savepath)

