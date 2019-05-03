from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
import logging

logger = logging.getLogger(__name__)

def walk_dict(node, indent=0):
    if isinstance(node, list):
        logger.info("{}{}".format("\t"*indent, "["))
        for item in node:
            walk_dict(item, indent+1)
        logger.info("{}{}".format("\t"*indent, "]"))
    elif isinstance(node, dict):
        logger.info("{}{}".format("\t"*indent, "{"))
        for key, value in node.items():
            if not value:
                logger.info("{}{}: EMPTY".format("\t"*indent, key))
            elif isinstance(value, list) or isinstance(value, dict):
                logger.info("{}{}:".format("\t"*indent, key))
                walk_dict(value, indent+1)
            else:
                logger.info("{}{}:{}".format("\t"*indent, key, value))
        logger.info("{}{}".format("\t"*indent, "}"))
    else:
        logger.info("{}{}".format("\t"*indent, node))

def scan_pickle(pickle_file):
    data = get_pickle_data(pickle_file)
    walk_dict(data)

#save_json("{}.json".format(base_name), data)
base_name = "kt_api_data"
#pickle_file = "kaptio_allsell.pickle"
pickle_file = "{}.pickle".format(base_name)
scan_pickle(pickle_file)

