# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json
import json
import pickle
import os
from time import time
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
kt_packages = data['packages']
kt_pricelist = data['pricelist']

occ_rev = {}
for key, value in occupancy.items():
    occ_rev[value] = key

tp_rev = {}
for key, value in tax_profiles.items():
    tp_rev[value] = key

sourecfile = 'kt_priceres_20190316075513.json'
kt_newprice = load_json(os.path.join(savepath, 'data', sourecfile))
kt_replaced = []
kt_toreplace = []

item_checked = 0
for key, value in kt_newprice.items():
    if isinstance(value, list):
        for item in value:
            item_checked += 1
            sv = item['search_values'].copy()
            tax_profile = tp_rev[sv['tax_profile_id']]
            occupancy_str = occ_rev["1={}".format(sv['occupancy'])]
            if 'results' in item:
                rd = item['results']
                packageid = rd['id']
                if 'prices_by_service_level' in rd:
                    for p_item in rd['prices_by_service_level']:
                        if len(p_item['errors']) == 0:
                            # we have a valid price...
                            datestr = item['date']
                            service_level_id = item['service_level_id']
                            n_data = {
                                        'packageid': packageid,
                                        'date': datestr,
                                        'occupancy': sv['occupancy'],
                                        'tax_profile_id': sv['tax_profile_id']
                                    }
                            kt_toreplace.append(n_data)

                            if packageid in kt_pricelist:
                                if 'pricelist' in kt_pricelist['packageid']:
                                    if datestr in kt_pricelist['packageid']['pricelist']:
                                        if tax_profile in kt_pricelist['packageid']['pricelist'][datestr]:
                                            if occupancy_str in kt_pricelist['packageid']['pricelist'][datestr][tax_profile]:
                                                for pl in kt_pricelist['packageid']['pricelist'][datestr][tax_profile][occupancy_str]:
                                                    if pl['service_level_id'] == service_level_id:
                                                        pl['errors'] = []
                                                        pl['total_price'] = p_item['total_price'].copy()
                                                        kt_replaced.append(n_data)

logger.info("Processed {} : replaced {}".format(item_checked, len(kt_replaced)))
data['pricelist'] = kt_pricelist
save_pickle_data(data, pickle_file)

file_path = os.path.join(savepath, "data", "kt_replaced_{}.json".format(timestamp))
save_json(file_path, kt_replaced)

file_path = os.path.join(savepath, "data", "kt_toreplace_{}.json".format(timestamp))
save_json(file_path, kt_toreplace)

