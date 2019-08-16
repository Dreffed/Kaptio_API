# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
from utils_packages import process_packages
import json
import pickle
import os
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
localpaths = ["data", "fresh"]
localpath = os.path.join(homepath, *localpaths)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

logger.info("Data keys loaded...")
for key, value in data.items():
    logger.info("\t{} => {} : {}".format(key, type(value), len(value)))

packageid = 'a754F0000000A30QAE'
channelid = 'a6H4F0000000DkMUAU'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
search_values = data['search_values']
season_start = data['season']['start']
season_end = data['season']['end']
kt_packages = data['packages']
kt_pricelist = data['pricelist']

kt_updates = process_packages(kaptioclient=kt, savepath=savepath, packages=kt_packages, tax_profiles=tax_profiles, channelid=channelid, occupancy=occupancy)
data['updates'] = kt_updates
save_pickle_data(data, pickle_file)

logger.info(json.dumps(kt_updates.get('counts', {}), indent=4))
file_path = os.path.join(savepath, "data", "kt_updates_{}.json".format(timestamp))
save_json(file_path, kt_updates)
