from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
from utils_dict import extract_rows
from utils_config import get_folderpath, load_config, get_configuration_path
from utils_processors import (
        backup_data, load_metadata, init_partial, 
        promote_custom, process_dates, process_prices, 
        process_packages, clear_data, process_content,
        process_items
    )
from utils_parallel import process_price_parallel
from utils_output import (
        process_allsell, process_bulkloader, 
        process_errors, process_xml
    )
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import socket
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

    run_data = {
        "homepath": PATHS.get('HOME', os.path.expanduser("~")),
        "localpath": PATHS.get('LOCAL', os.getcwd()),
        "pid": os.getpid(),
        "date": datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
        "server": socket.gethostname(),
        "processes": []
    }

    logger.info("Timestamp: {}".format(run_data.get('run_data',{}).get('timestamp')))

    savepath = get_folderpath(config, '_remote', PATHS)
    logger.info('Savepath: {}'.format(savepath))

    kaptio_config_file = get_configuration_path(config, 'kaptio', PATHS)
    logger.info('KT Config: {}'.format(kaptio_config_file))
    kaptio_config = load_kaptioconfig(kaptio_config_file)

    baseurl = kaptio_config['api']['baseurl']
    
    run_data['baseurl'] = baseurl

    kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

    pickle_file = get_configuration_path(config, 'pickle', PATHS)
    data = get_pickle_data(pickle_file)
    run_data['pickle'] = pickle_file

    function_switch = {
        'backup': backup_data,
        'clear_data': clear_data,
        'partial': init_partial,
        'metadata': load_metadata,
        'packages': process_packages,
        'custom': promote_custom,
        'dates': process_dates,
        'prices': process_prices,
        'price_para': process_price_parallel,
        'errors': process_errors,
        'content': process_content,
        'items': process_items,
        'allsell': process_allsell,
        'bulkloader': process_bulkloader,
        'xml': process_xml
    }

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
        run_data['processes'].append(process)
        #try:
        if function_switch.get(process):
            data = function_switch.get(process)(config, data, kt, savepath)
        else:
            logging.warning("no process defined for {}".format(process))
        #except Exception as ex:
        #    logger.error('=== ERROR: {} => {}'.format(process, ex))
        #    break

    run_data['end'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    if not data.get('_runs'):
        data['_runs'] = {}

    run_name = "{}-{}".format(run_data.get('hostname'), run_data.get('date'))
    data['_runs'][run_name] = run_data

    logger.info("Data keys loaded...")
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
        else:
            logger.info("\t{} : No Values".format(key))

    save_pickle_data(data, pickle_file)
    save_json("kt_api_data.json", data)

if __name__ == '__main__':
    main()
