
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import os
import path
from time import time
from datetime import datetime

datapaths = ["C:/", "Users", "David Gloyn-Cox", "OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(*datapaths)
print(savepath)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']
packageid = ''

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print("Timestamp: {}".format(timestamp))

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
        "quad_2child":"1=2,2",
        "quad_3child":"1=1,3"
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
        "service_level_ids":"a7r4F0000000AloQAE",     #,a7r4F0000004fd2QAA,a7r4F0000004nVbQAI
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
    kt_packages = kt.get_packages(savepath)
    print("\tFetched packages [{}]".format(len(kt_packages)))
    data['packages'] = kt_packages

kt_packages_short = kt_packages[:50]

print("Calls per package: {}".format(len(tax_profiles) * len(occupancy)))
kt_pricelist = kt.process_packages_pool(savepath=savepath, 
                    packages=kt_packages, 
                    tax_profiles=tax_profiles, 
                    occupancy=occupancy, 
                    worker_count=10, 
                    limit=0,
                    debug=True)

for item in kt_pricelist:
    print(item['id'], item['name'])

file_path = os.path.join(savepath, "data", "kt_pricelist_{}.json".format(timestamp))
save_json(file_path, kt_pricelist)

