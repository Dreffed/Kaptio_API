# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
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

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

packageid = 'xxxxxxxxxxxxxxxxx'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

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

filepath = os.path.join(savepath, 'data', 'kt_priceerrors_20190314233403.json' )
with open(filepath, 'r') as fp:
    kt_errorlist = json.load(fp)

logger.info("processing {} query strs".format(len(kt_errorlist)))

re_q = re.compile(r"id==([a-zA-Z0-9]+)[&]{0,1}")
kt_pricesres = {}
errorlist = []
progress = 0
processed = 0
errors = 0
for q in kt_errorlist:
    progress += 1
    search_values = {}
    phrases = q[1:].split("&")
    for phrase in phrases:
        terms = phrase.split("=")
        search_values[terms[0]] = terms[-1]
        
    if 'filter' in search_values:
        packageid = search_values['filter']

    if 'tax_profile_id' in search_values:
        if search_values['tax_profile_id'] in tax_profiles:
            old_tp = search_values['tax_profile_id']
            search_values['tax_profile_id'] = tax_profiles[search_values['tax_profile_id']]
            q = q.replace(old_tp, search_values['tax_profile_id'])

    try:
        p = kt.get_packageprice_query(savepath, packageid, q)
        if not packageid in kt_pricesres:
            kt_pricesres[packageid] = []
        p_data = {
            "search_values": search_values,
            "response": p
            }
        kt_pricesres[packageid].append(p_data)
        processed +1
    except Exception as ex:
        logger.info("ERROR: {}\n\t{}\n\t{}".format(q,search_values,ex))
        errorlist.append({
            "q":q,
            "search_vlaues":search_values,
            "error": ex
        })
        errors += 1
    if progress % 100 == 0:
        logger.info("{}/{} :: E:{}".format(processed, progress, errors))

logger.info("Fetched {} prices".format(len(kt_pricesres)))

file_path = os.path.join(savepath, "data", "kt_priceres_{}.json".format(timestamp))
save_json(file_path, kt_pricesres)


