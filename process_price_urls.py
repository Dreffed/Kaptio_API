# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import re

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

packageid = 'xxxxxxxxxxxxxxxxx'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print("Timestamp: {}".format(timestamp))
 
filepath = os.path.join(savepath, 'data', 'kt_priceerrors_20190314233403.json' )
with open(filepath, 'r') as fp:
    kt_errorlist = json.load(fp)

print("processing {} query strs".format(len(kt_errorlist)))

re_q = re.compile(r"id==([a-zA-Z0-9]+)[&]{0,1}")
kt_pricesres = {}
for q in kt_errorlist:
    print(q)
    m = re_q.search(q)
    if m:
        packageid = m.group(1)

    p_data = kt.get_packageprice_query(savepath, packageid, q, debug=True)
    if not packageid in kt_pricesres:
        kt_pricesres[packageid] = []
    kt_pricesres[packageid].append(p_data)
    break

file_path = os.path.join(savepath, "data", "kt_priceres_{}.json".format(timestamp))
save_json(file_path, kt_pricesres)


