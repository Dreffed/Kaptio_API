from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from kaptiorestpython.ograph import KaptioOGraph
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
print(savepath)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['ograph']['baseurl']
sfurl = kaptio_config['sf']['url']
username = kaptio_config['sf']['username']
password = kaptio_config['sf']['passwd']
security_token = kaptio_config['sf']['token']
sandbox = True
clientid = kaptio_config['ograph']['clientid']
clientsecret = kaptio_config['ograph']['clientsecret']

kt = KaptioOGraph(baseurl, sfurl, username, password, security_token, sandbox, clientid, clientsecret)

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

print("Available items:")
for key, value in data.items():
    print("\t{}".format(key))

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print("Timestamp: {}".format(timestamp))

tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
search_values = data['search_values']
season_start = data['season']['start']
season_end = data['season']['end']
kt_packages = data['packages']

kt_content = {}
if not 'content' in data:
    for p in kt_packages:
        # get the content
        if not p['id'] in kt_content:
            kt_content[p['id']] = kt.get_content(p['id'])
    data['content'] = kt_content
    save_pickle_data(data, pickle_file)
else:
    kt_content = data['content']

print("Found content for {}".format(len(kt_content)))

# extract out the fields from the content...
