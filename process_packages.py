# load the dependancies
from kaptiorestpython import KaptioClient, load_kaptioconfig, save_json
import json
import pickle
import os
import path
import pandas as pd
import requests
from time import time
from datetime import datetime
from multiprocessing import Lock, Process, Queue, current_process
import queue

datapaths = ["C:/", "Users", "David Gloyn-Cox", "OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(*datapaths)
print(savepath)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)
headers = {'Authorization': 'Keypair key={} secret={}'.format(kaptio_config['api']['auth']['key'], 
                                                                kaptio_config['api']['auth']['secret']),
          "Content-Type":"application/json"}

debug = True
baseurl = kaptio_config['api']['baseurl']

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'

if 'tax_profiles' in data:
    tax_profiles = data['tax_profiles']
else:
    # set up walker elements
    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA",
        "Foreign":"a8H4F0000003uJbUAI",
        "Domestic":"a8H4F0000003tnfUAA"
    }
    data['tax_profiles'] = tax_profiles

if 'occupancy' in data:
    occupancy = data['occupancy']
else:
    occupancy = {
        "single":"1=1,0",
        "double":"1=2,0",
        "double_child":"1=1,1",
        "triple":"1=3,0",
        "triple_1child":"1=2,1",
        "triple_2child":"1=1,2",
        "quad":"1=4,0",
        "quad_1child":"1=3,1",
        "quad_1child":"1=2,2",
        "quad_1child":"1=1,3"
    }
    data['occupancy'] = occupancy

if 'search_values' in data:
    search_values = data['search_values']
else:
    search_values = {
        "tax_profile_id":"a8H4F0000003tsfUAA",        # Required    #Zero
        "channel_id":'a6H4F0000000DkMUAU',            # Required    # travel agent
        "currency":"CAD",                             # Required
        "occupancy":"1=1,0",                          # Required
        "service_level_ids":"a7r4F0000000AloQAE", #,a7r4F0000004fd2QAA,a7r4F0000004nVbQAI
        "date_from":"2020-03-01",
        "date_to":"2020-10-31",
        "mode":"all",
        "filter":"id=={}".format(packageid)
    }
    data['search_values'] = search_values

if not 'season' in data:
    data['season'] = {}
    data['season']['start'] = '2020-04-01'
    data['season']['end'] = '2020-10-31'

season_start = data['season']['start']
season_end = data['season']['end']

if 'packages' in data:
    kt_packages = data['packages']
else:
    kt_packages = get_packages(headers, savepath, baseurl)
    print("\tFetched packages".format(len(kt_packages)))
    data['packages'] = kt_packages

for p in kt_packages:
    if not 'dates' in p:
        print("Fetching dates...{} => {}".format(p['id'], p['name']))
        p['dates'] = get_packagedepartures(headers, savepath, baseurl, p['id'], season_start, season_end)    
    
    # deal wit hteh custom fields
    for key, value in p['custom_fields'].items():
        if not key in p:
            p[key] = value

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)

if 'pricelist' in data:
    kt_pricelist = data['pricelist']
else:
    # do a short load....
    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA"
    }
    occupancy = {
        "single":"1=1,0"
    }
    # run the pricelist load...
    kt_pricelist = get_extract(headers, savepath, baseurl, kt_packages, tax_profiles, occupancy, debug)
    data['pricelist'] = kt_pricelist

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)

# augment the kt_rpicelist with the pricelist info
for p in kt_packages:
    if not 'pricelist' in p:
        if 'id' in kt_pricelist:
            p['pricelist'] = kt_pricelist[p['id']]

file_path = os.path.join(savepath, "data", "kt_pricelist.json")
save_json(file_path, kt_pricelist)

file_path = os.path.join(savepath, "data", "kt_packages_aug.json")
save_json(file_path, kt_packages)

"""
# short list for proof
tax_profiles = {
    "Zero Rated":"a8H4F0000003tsfUAA"
}
occupancy = {
    "single":"1=1,0"
}

s_dates = list(sorted(kt_dates))[:1]
print("Calls per package: {}".format(len(tax_profiles) * len(occupancy) * len(s_dates)))

kt_augpackages = get_extract(headers, savepath, baseurl, kt_packages, s_dates, tax_profiles, occupancy)
for key, value in kt_augpackages.items():
    print(key)
"""

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)