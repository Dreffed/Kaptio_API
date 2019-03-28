from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def display_data(pickle_file, data, name):
    logger.info("{}: {}".format(name, pickle_file))
    for key, value in data.items():
        logger.info("\t{} => {}".format(key, len(value)))

scan_local = False
scan_remote = False
update_lookups = False
get_remote_content = False
check_updates = True

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

if scan_local:
    logger.info("Local Pickles:")
    for f in scanfiles('.', r".*\.pickle"):
        logger.info("\t{} => {}".format(f['file'], f['folder']))

if scan_remote:
    logger.info("Remote Pickles:")
    for f in scanfiles(os.path.join(savepath, 'config'), r".*\.pickle"):
        logger.info("\t{} => {}".format(f['file'], f['folder']))

pickle_file = "kaptio_allsell.pickle"

data = get_pickle_data(pickle_file)

display_data(pickle_file, data, "Existing")

source_pickle_file = os.path.join(savepath, "config", "kaptio_allsell.Parandarus.201903230942.pickle")
data_src = get_pickle_data(source_pickle_file)

display_data(source_pickle_file, data_src, "Remote")

if get_remote_content and 'content' in data_src:
    logger.info("Fetching remote cached content")
    kt_content = data_src.get('content')

    if kt_content:
        data['content'] = kt_content
        logger.info("Retrieved remote cached content")

        save_pickle_data(data, pickle_file)

if check_updates and  'updates' in data:
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
        
display_data(pickle_file, data, "Updated")
