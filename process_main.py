from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
from utils_config import get_folderpath, load_config, get_configuration_path
from utils_processors  import load_metadata, init_partial, promote_custom, process_dates, process_prices, process_packages
import json
import pickle
import os
import path
from time import time
from datetime import datetime
  
PATHS = {
    'LOCAL': os.getcwd(),
    'HOME': os.path.expanduser("~") 
}

config = load_config('ktapi.json')

log = config.get('flags', {}).get('switches', {}).get('logging')

homepath = PATHS.get('HOME', os.path.expanduser("~"))
localpath = PATHS.get('LOCAL', os.getcwd())
pid = os.getpid()

savepath = get_folderpath(config, '_remote', PATHS)
if log:
    print('Savepath: {}'.format(savepath))

kt_setup = config.get('configurations',{}).get('kaptio')
kt_config_path = get_folderpath(config, kt_setup.get('folder'), PATHS)

kaptio_config_file = get_configuration_path(config, 'kaptio', PATHS)
if log:
    print('KT Config: {}'.format(kaptio_config_file))
kaptio_config = load_kaptioconfig(kaptio_config_file)

baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = get_configuration_path(config, 'pickle', PATHS)
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
if log:
    print("Timestamp: {}".format(timestamp))

function_swtich = {
    'partial': init_partial,
    'metadata': load_metadata,
    'packages': process_packages,
    'custom': promote_custom,
    'dates': process_dates,
    'prices': process_prices,
    #'errors': process_errors,
    #'content': process_content,
    #'items': process_items,
    #'allsell': process_allsell,
    #'bulkloader': process_bulkloader,
    #'xml': process_xml
}

if config.get('flags', {}).get('switches', {}).get('full') or config.get('flags', {}).get('switches', {}).get('reload'):
    if log:
        print("reloading data...")
    data = {}
    save_pickle_data(data, pickle_file)

if log:
    if len(data)> 0:
        print("Data keys loaded...")
        for key, value in data.items():
            if value:
                print("\t{} => {} : {}".format(key, type(value), len(value)))
            else:
                print("\t{} : No Values".format(key))
    else:
        print('Clean data file...')

for process in config.get('process', []):
    #try:
    if function_swtich.get(process):
        data = function_swtich.get(process)(config, data, kt, savepath)
    #except Exception as ex:
    #    print('=== ERROR: {} => {}'.format(process, ex))

if log:
    print("Data keys loaded...")
    for key, value in data.items():
        if value:
            print("\t{} => {} : {}".format(key, type(value), len(value)))
        else:
            print("\t{} : No Values".format(key))

save_pickle_data(data, pickle_file)
if log:
    save_json("kt_api_data.json", data)

