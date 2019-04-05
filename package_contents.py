from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from kaptiorestpython.ograph import KaptioOGraph
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
logger.info(savepath)

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

pickle_file = "kt_api_data_CAD.pickle"
data = get_pickle_data(pickle_file)

logger.info("Available items:")
for key, value in data.items():
    logger.info("\t{}".format(key))

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

kt_packages = data['packages']
for key, value in data.items():
    if value:
        print("\t{} -> {}:{}".format(key, type(value), len(value)))
    else:
        print("\t{} -> {}:{}".format(key, "NONE", 0))

kt_content = {}
if 'content' in data:
    del data['content']
    
if not 'content' in data:
    for p in kt_packages:
        # get the content
        if not p['id'] in kt_content:
            kt_content[p['id']] = kt.get_content(p['id'])
    data['content'] = kt_content
    save_pickle_data(data, pickle_file)
else:
    kt_content = data['content']

logger.info("Found content for {}".format(len(kt_content)))

# extract out the fields from the content...
