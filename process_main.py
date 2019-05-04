from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from kaptiorestpython.ograph import KaptioOGraph
from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
from utils_dict import extract_rows
from utils_config import get_folderpath, load_config, get_configuration_path
from utils_processors import (
        save_data, backup_data, load_metadata, init_partial, update_taxprofiles,
        promote_custom, process_dates, process_prices, 
        process_packages, clear_data, process_content,
        process_items, augment_pricelists, get_ktapi, get_ograph
    )
from utils_parallel import process_price_parallel
from utils_output import (
        process_allsell, process_pricedata, process_bulkloader, 
        process_errors, process_xml
    )
from utils_pickle import export_pickle
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
    config["paths"] = PATHS

    run_data = {
        "homepath": PATHS.get('HOME', os.path.expanduser("~")),
        "localpath": PATHS.get('LOCAL', os.getcwd()),
        "pid": os.getpid(),
        "date": datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
        "server": socket.gethostname(),
        "processes": []
    }

    logger.info("Timestamp: {}".format(run_data.get('date')))

    savepath = get_folderpath(config, '_remote', PATHS)
    logger.info('Savepath: {}'.format(savepath))

    config_type = config.get("configurations", {}).get("run", {}).get("kaptio")
    kaptio_config_file = get_configuration_path(config, config_type, config.get('paths', []))
    logger.info("\tLoading config: {}".format(kaptio_config_file))

    kaptio_config = load_kaptioconfig(kaptio_config_file)    
    baseurl = kaptio_config['api']['baseurl']
    
    run_data['baseurl'] = baseurl

    kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

    function_switch = {
        'save_data': save_data,
        'backup': backup_data,
        'export': export_pickle,
        'clear_data': clear_data,
        'partial': init_partial,
        'metadata': load_metadata,
        'tax_profiles': update_taxprofiles,
        'packages': process_packages,
        'custom': promote_custom,
        'dates': process_dates,
        'prices': process_prices,
        'price_para': process_price_parallel,
        'augment_price': augment_pricelists,
        'errors': process_errors,
        'content': process_content,
        'items': process_items,
        'allsell': process_pricedata,
        'bulkloader': process_bulkloader,
        'xml': process_xml
    }

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

        if len(data)> 0:
            logger.info("Data keys loaded...")
            for key, value in data.items():
                if value:
                    logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
                else:
                    logger.info("\t{} : No Values".format(key))

        run_data['pickle'] = pickle_file

        for process in config.get('process', []):
            logger.info("Running: {}".format(process))
            run_data['processes'].append(process)
            #try:
            if function_switch.get(process):
                data = function_switch.get(process)(config, data, kt, savepath)
            else:
                logging.warning("no process defined for {}".format(process))
            #except Exception as ex:
            #    logger.error('=== ERROR: {} => {}\n\tSTOPPING!'.format(process, ex))
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
        try:
            save_json("kt_api_data.json", data)
        except Exception as ex:
            logger.info("Failed to save JSON file.\n\t{}".format(ex))

if __name__ == '__main__':
    main()
