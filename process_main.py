from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
from utils_config import get_folderpath, load_config, get_configuration_path
from utils_processors  import load_metadata, init_partial, promote_custom, process_dates, process_prices, process_packages
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging
import logging.config

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

homepath = PATHS.get('HOME', os.path.expanduser("~"))
localpath = PATHS.get('LOCAL', os.getcwd())
pid = os.getpid()

savepath = get_folderpath(config, '_remote', PATHS)
logger.info('Savepath: {}'.format(savepath))

kt_setup = config.get('configurations',{}).get('kaptio')
kt_config_path = get_folderpath(config, kt_setup.get('folder'), PATHS)

kaptio_config_file = get_configuration_path(config, 'kaptio', PATHS)
logger.info('KT Config: {}'.format(kaptio_config_file))
kaptio_config = load_kaptioconfig(kaptio_config_file)

baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = get_configuration_path(config, 'pickle', PATHS)
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

function_switch = {
    'partial': init_partial,
    'metadata': load_metadata,
    'packages': process_packages,
    'custom': promote_custom,
    'dates': process_dates,
    'prices': process_prices,
    #'errors': process_errors,
    #'content': process_content,
    #'items': process_items,
    #'allsell': process_allsell,
    #'bulkloader': process_bulkloader,
    #'xml': process_xml
}

if config.get('flags', {}).get('switches', {}).get('full') or config.get('flags', {}).get('switches', {}).get('reload'):
    logger.info("reloading data...")
    data = {}
    save_pickle_data(data, pickle_file)

if logger.level == logging.DEBUG and len(data)> 0:
    logger.info("Data keys loaded...")
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
        else:
            logger.info("\t{} : No Values".format(key))
else:
    logger.info('Clean data file...')

for process in config.get('process', []):
    logger.info("Running: {}".format(process))
    try:
        if function_switch.get(process):
            data = function_switch.get(process)(config, data, kt, savepath)
        else:
            logging.warning("no process defined for {}".format(process))
    except Exception as ex:
        logger.error('=== ERROR: {} => {}'.format(process, ex))
        break

logger.info("Data keys loaded...")
for key, value in data.items():
    if value:
        logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
    else:
        logger.info("\t{} : No Values".format(key))

save_pickle_data(data, pickle_file)
save_json("kt_api_data.json", data)

