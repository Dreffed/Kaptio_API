
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import os
import path
from time import time
from datetime import datetime

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print("Timestamp: {}".format(timestamp))

tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
search_values = data['search_values']
season_start = data['season']['start']
season_end = data['season']['end']
kt_packages = data['packages']

# check the correct data is there...
for p in kt_packages:
    if not 'tax_profiles' in p:
        p['tax_profiles'] = tax_profiles
    if not 'occupancy' in p:
        p['occupancy'] = occupancy

save_pickle_data(data, pickle_file)

kt_pricelist = kt.run_pool(pool_thread=10, pacakges=kt_packages)

file_path = os.path.join(savepath, "data", "kt_pricelist_{}.json".format(timestamp))
save_json(file_path, kt_pricelist)

