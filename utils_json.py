import json
import os
from utils import get_pickle_data, save_pickle_data, save_json, load_json, scanfiles
from utils_config import get_folderpath, load_config, get_configuration_path
from utils_processors import backup_data
import logging

def recover_json(config, paths):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    currencies = config.get("presets", {}).get("currencies", ["CAD"])
    for currency in currencies:
        config['presets']['currency'] = currency
        config_type = config.get("configurations", {}).get("run", {}).get("pickle")
        pickle_file = get_configuration_path(config, config_type, paths)
        name, ext = os.path.splitext(pickle_file)
        json_file = "{}_{}{}".format(name,currency, ".json")
        pickle_file = "{}_{}{}".format(name,currency, ext)

        logger.info("Files\n\tpickle: {}\n\tjson: {}".format(pickle_file, json_file))
        if not 'presets' in config:
            config['presets']

        config['presets']['pickle'] = pickle_file
        config['presets']['json'] = json_file

        if os.path.exists(json_file):
            data = load_json(json_file)
            save_pickle_data(data, pickle_file)
        else:
            logger.error("missing path {}".format(json_file))
                    
def main():
    logger = logging.getLogger(__name__)

    PATHS = {
        'LOCAL': os.getcwd(),
        'HOME': os.path.expanduser("~") 
    }

    config = load_config('ktapi.json')
    try:
        logging.config.dictConfig(config.get('logger', {}))
    except:
        logging.basicConfig(level=logging.INFO)

    savepath = get_folderpath(config, '_remote', PATHS)
    logger.info('Savepath: {}'.format(savepath))

    backup_data(config, None, None, savepath)

    recover_json(config, PATHS)

    backup_data(config, None, None, savepath)


if __name__ == "__main__":
    main()