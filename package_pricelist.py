# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, scan_packagefiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime

reload = True
checkdumps = False

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
kt_processed = {}

if checkdumps:
    kt_processed = scan_packagefiles(savepath)

# default behaviour is to only reload errored packages
if 'pricelist' in data and not reload:
    error_count = 0
    kt_pricelist = data['pricelist']
    print("Loaded {} packages".format(len(kt_packages)))

    for p in kt_packages:
        if kt_processed.get(packageid):
            continue

        if 'pricelist' in p:
            if 'errors' in p['pricelist']:
                #print("{} has pricelist".format(p['id']))
                #print("\tERROR Found: {}".format(p['pricelist']['errors']))    
                error_count += p['pricelist']['errors']

    if error_count > 0:
        print("Load errors found, rerunning {}".format(error_count))
        kt_pricelist = kt.get_extract(savepath, kt_packages, tax_profiles, occupancy, debug)
        data['pricelist'] = kt_pricelist
        save_pickle_data(data, pickle_file)

else:
    # do a short load....
    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA",
        "Foreign":"a8H4F0000003uJbUAI",
        "Domestic":"a8H4F0000003tnfUAA"
    }
    occupancy = {
        "single":"1=1,0",
        "double":"1=2,0",
        "triple":"1=3,0",
        "quad":"1=4,0"    
    }
    
    # run the pricelist load...
    kt_pricelist = kt.get_extract(savepath, kt_packages, tax_profiles, occupancy, debug)
    data['pricelist'] = kt_pricelist
    save_pickle_data(data, pickle_file)

# augment the kt_rpicelist with the pricelist info
for p in kt_packages:
    if not 'pricelist' in p:
        if 'id' in kt_pricelist:
            p['pricelist'] = kt_pricelist[p['id']]

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)

file_path = os.path.join(savepath, "data", "kt_pricelist_{}.json".format(timestamp))
save_json(file_path, kt_pricelist)

file_path = os.path.join(savepath, "data", "kt_augmented_{}.json".format(timestamp))
save_json(file_path, kt_packages)
