from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json
from utils_dict import extract_rows
from utils_config import get_folderpath, load_config, get_configuration_path
import json
import pickle
import os

from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def display_data(pickle_file, data, name):
    logger.info("{}: {}".format(name, pickle_file))
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} : {}".format(key, type(value), len(value)))
        else:
            logger.info("\t{} : No Values".format(key))

def main():
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

    scan_local = config.get("flags", {}).get("switches", {}).get("scan_local", False)
    scan_remote = config.get("flags", {}).get("switches", {}).get("scan_remote", False)
    get_remote_content = config.get("flags", {}).get("switches", {}).get("import_remote", False)
    check_updates = config.get("flags", {}).get("switches", {}).get("check_updates", False)

    savepath = get_folderpath(config, '_remote', PATHS)
    logger.info('Savepath: {}'.format(savepath))
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("Timestamp: {}".format(timestamp))

    logger.info("Runnins:\n\tscan local\t{}\n\tscan remote\t{}\n\tget remote\t{}\n\tcheck updates\t{}".format(scan_local, scan_remote, get_remote_content, check_updates))

    if scan_local:
        logger.info("Local Pickles:")
        for f in scanfiles('.', r".*\.pickle"):
            logger.info("\t{} => {}".format(f['file'], f['folder']))

    if scan_remote:
        logger.info("Remote Pickles:")
        for f in scanfiles(os.path.join(savepath, 'config'), r".*\.pickle"):
            logger.info("\t{} => {}".format(f['file'], f['folder']))

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

        if get_remote_content:
            config_type = config.get("configurations", {}).get("run", {}).get("remote_pickle")
            if not config_type:
                logger.error("\tUnable to local remote pickle details {configurations->run->remote_pickle}")
                break

            remote_pickle = get_configuration_path(config, config_type, config.get('paths', []))
            logger.info("\tloading remote {}".format(remote_pickle))

            data_src = get_pickle_data(remote_pickle)

            if  'content' in data_src:
                logger.info("Fetching remote cached content")
                kt_content = data_src.get('content')

                if kt_content:
                    data['content'] = kt_content
                    logger.info("Retrieved remote cached content")

                    save_pickle_data(data, pickle_file)
        display_data(pickle_file=pickle_file, data=data, name="Local")

        if check_updates:
            update_prices(data=data, data_src=data_src)
        
def update_prices(data, data_src):
        kt_packages = data.get('packages', [])
        kt_pricelist = data.get('pricelist', [])
        kt_updates = {}
        count_1020 = len(data.get('updates', {}).get('logs', {}).get('1020', []))
        count_1040 = len(data.get('updates', {}).get('logs', {}).get('1040', []))
        count_fixed = len(data.get('updates', {}).get('prices', []))
        logger.info("Updates\n\t1040\t{}\n\t1020\t{}\n\tFixed\t{}".format(count_1040, count_1020, count_fixed))

        packageids = set()
        tentwentyids = set()

        for n_item in data.get('updates', {}).get('prices', []):
            logger.debug(len(n_item))
            """ 
                'packageid': packageid,
                'date': d_key,
                'tax_profile_id': taxprofileid,
                'tax_profile': t_key,
                'occupancy': o_key,
                'occupancy_id': occ_str,
                'service_level_id': p_item.get('service_level_id'),
                'service_level': sl_rev.get(p_item.get('service_level_id'),{}).get('name'),
                'code': e_code,
                'message': e_item.get('error', {}).get('message') 
                'data': array of prices found, should be one only
            """
            packageids.add(n_item.get('packageid'))
        
        for n_item in data.get('updates', {}).get('logs', {}).get('1020'):
            tentwentyids.add(n_item.get('packageid'))

        stillids = tentwentyids.difference(packageids)

        for id in stillids:
            logger.info(id)
            

if __name__ == "__main__":
    main()
