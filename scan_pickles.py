from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
from utils_config import get_folderpath, load_config, get_configuration_path

import os
import logging
import logging.config

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
    config["paths"] = PATHS

    folderpath = get_folderpath(config, '_config', PATHS)
    logging.info("Scanning folder: {}".format(folderpath))

    for f in scanfiles(folder=folderpath, filter=".pickle$"):
        filepath = os.path.join(f.get('folder', ''), f.get('file'))
        data = get_pickle_data(pickleName=filepath)
        for k,v in data.items():
            if v:
                logger.info("\t{} => {} : {}".format(k, type(v), len(v)))
            else:
                logger.info("\t{}".format(k))

        content = data.get('content')
        if content:
            filename, _ = os.path.splitext(f.get('file'))
            filename = '{}.json'.format(filename)
            save_json(file_path=filename, data=content)
            logger.info("Export: {} => {}".format('content', filename))
            

if __name__ == "__main__":
    main()