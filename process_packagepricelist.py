# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
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
kt_pricelist = data['pricelist']

# get teh file list
datapath = os.path.join(savepath, 'data')
fn_pricelist = []
fn_reprocess = []
filecount = 0
errorcount = 0
for f in scanfiles(datapath, r'price_([a-zA-Z0-9]+)_([\d]+)\.json'):
    filepath = os.path.join(f['folder'], f['file'])
    try:
        filecount += 1
        with open(filepath) as fp:
            try:
                pl_data = json.load(fp)
                fn_pricelist.append(pl_data)
            except:
                raw_data = fp.read()
                fn_reprocess.append(raw_data)
                print(filepath)
                print(raw_data)
                break
                
    except Exception as ex:
        print("ERROR: {}\n\t{}".format(json.dumps(f, indent=4), ex))
        errorcount += 1

print("Loaded {} JSON {} RAW {}\n\t=={} errors".format(filecount, len(fn_pricelist), len(fn_reprocess), errorcount))

# now reprocess the raw_file:
errorcount = 0
filecount = 0
fn_errors = []
for r_data in fn_reprocess:
    t_data = "[{}]".format(r_data.replace("}{", "},{"))
    try:
        pl_data = json.loads(t_data)
        fn_pricelist.append(pl_data)
        filecount += 1
        print(json.dumps(pl_data, indent=4))
        print(t_data)
        print(r_data)
        break
    except Exception as ex:
        print("ERROR: {}\n\t{}".format(json.dumps(f, indent=4), ex))
        errorcount += 1
        err = {"error:": ex, "data": r_data}
        fn_errors.append(err)
        
print("Reloaded {} JSON {}\n\t=={} / {} errors".format(filecount, len(fn_pricelist), errorcount, len(fn_errors)))
fn_reprocess = None

# load the data into memory, index by query...
kt_prices = {}
for p_data in fn_pricelist:
    if isinstance(p_data, list):
        print("Array: {}".format(len(p_data)))
        print(json.dumps(p_data, indent=4))
        for item in p_data:
            if not item['query'] in kt_prices:
                kt_prices[item['query']] = item
            else:
                print("Already indexed...")
        break
    elif isinstance(p_data, dict):
        kt_prices[p_data['query']] = p_data
    else:
        print("incorrect data! {}".format(p_data))

print("Extracted: {} calls".format(len(kt_prices)))

# load in the errors from the pickle file...
error_list = []
for p in kt_packages:
    if 'pricelist' in p:
        p_data = p['pricelist']
        if 'errors' in p_data:
            #print(p['id'], p['name'])
            error_list.append(p_data)

print(len(error_list))
    
    
    

