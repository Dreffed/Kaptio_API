# load the dependancies
from kaptiorestpython.client import KaptioClient
from kaptiorestpython.utils_kaptio import load_kaptioconfig
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

def build_query(packageid, date_from, date_to, taxprofileid = 'a8H4F0000003tsfUAA', channelid = 'a6H4F0000000DkMUAU', 
                        occupancy = '1=1,0', services = 'a7r4F0000000AloQAE', currency="CAD"):
    querystr = ''
    search_values = {
        "tax_profile_id":taxprofileid,  # Required    #Zero
        "channel_id":channelid,         # Required    # travel agent
        "currency":currency,               # Required
        "occupancy":occupancy,          # Required
        "service_level_ids":services,   
        "date_from":date_from,
        "date_to":date_to,
        "mode":"",
        "filter":"id=={}".format(packageid)
    }

    search_list = []
    for key, value in search_values.items():
        if len(value) > 0:
            search_list.append("{}={}".format(key, value))

    if len(search_list) > 0:
        querystr = "?{}".format("&".join(search_list))    

    return querystr

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
localpaths = ["data", "fresh"]
localpath = os.path.join(homepath, *localpaths)

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
search_values = data['search_values']
season_start = data['season']['start']
season_end = data['season']['end']
kt_packages = data['packages']
kt_pricelist = data['pricelist']

rev_occ = {}
for key, value in occupancy.items():
    rev_occ[value] = key

# get teh file list
datapath = localpath #os.path.join(savepath, 'data')
fn_pricelist = []
fn_reprocess = []
filecount = 0
errorcount = 0
logger.info("scanning... {}".format(datapath))
for f in scanfiles(datapath, r'price_([a-zA-Z0-9]+)_([\d]+)\.json'):
    filepath = os.path.join(f['folder'], f['file'])
    try:
        filecount += 1
        with open(filepath, mode='r') as fp:
            try:
                pl_data = json.load(fp)
                fn_pricelist.append(pl_data)
                continue
            except Exception as ex:
                pass

            try:
                pl_data = json.loads("[{}]".format(fp.read().replace("}{", "},{")))
                fn_pricelist.append(pl_data)
            except Exception as ex:
                logger.info("Reprocess {}".format(ex))
                fn_reprocess.append(filepath)
                break
                
    except Exception as ex:
        logger.info("ERROR: {}\n\t{}".format(json.dumps(f, indent=4), ex))
        errorcount += 1

logger.info("Loaded {} JSON {} RAW {}\n\t=={} errors".format(filecount, len(fn_pricelist), len(fn_reprocess), errorcount))

# now reprocess the raw_file:
errorcount = 0
filecount = 0
fn_errors = []

# reprocess if we want
if True:
    logger.info("Missed {} files".format(len(fn_reprocess)))
else:        
    logger.info("Reloaded {} JSON {}\n\t=={} / {} errors".format(filecount, len(fn_pricelist), errorcount, len(fn_errors)))
fn_reprocess = None

# load the data into memory, index by query...
kt_prices = {}
for p_data in fn_pricelist:
    if isinstance(p_data, list):
        logger.debug("Array: {}".format(len(p_data)))
        logger.debug(json.dumps(p_data, indent=4))
        for item in p_data:
            if not item['query'] in kt_prices:
                kt_prices[item['query']] = item
            else:
                logger.info("Already indexed...")
    elif isinstance(p_data, dict):
        kt_prices[p_data['query']] = p_data
    else:
        logger.info("incorrect data! {}".format(p_data))

fn_pricelist = None

logger.info("Extracted: {} calls".format(len(kt_prices)))
logger.info(json.dumps(search_values, indent = 4))

channel_id = search_values['channel_id']
currency =  search_values['currency']

# load in the errors from the pickle file...
error_list = []
replaced = 0
for p in kt_packages:
    packageid = p['id']
    p_data = None
    if packageid in kt_pricelist:
        if 'pricelist' in kt_pricelist[packageid]:
            p_data = kt_pricelist[packageid]['pricelist']
        else:
            kt_pricelist[packageid]['pricelist'] = {}
    else:
        kt_pricelist[packageid] = {}
        kt_pricelist[packageid]['pricelist'] = {}

    if 'pricelist' in p and p_data is None:
            p_data = p['pricelist']

    if p_data:
        services_str = ",".join(p['services'])

        if 'errors' in p_data:
            for d_key, d_value in p_data.items():
                if d_key == 'errors':
                    continue
                for t_key, t_value in d_value.items():
                    for o_key, o_value in t_value.items():
                        retry = False
                        occ = occupancy[o_key]
                        querystr = build_query(packageid=packageid, 
                                            date_from=d_key, date_to=d_key, 
                                            taxprofileid=t_key, 
                                            channelid=channel_id, 
                                            occupancy=occ, 
                                            services=services_str)
                        for item in o_value:
                            try:
                                if len(item['errors']) > 0:
                                    for err in item['errors']:
                                        if err['error']['code'] == 1020:
                                            retry = True
                            except:
                                pass
                        if retry:
                            if querystr in kt_prices:
                                pl_data = kt_prices['resp']['results'][0]['prices_by_service_level']
                                p_data[d_key][t_key][o_key] = pl_data
                                replaced += 1
                            else:
                                error_list.append(querystr)
        p['pricelist'] = p_data
        kt_pricelist[packageid]['pricelist'] = p_data

logger.info("Replaced: {} Errors {}".format(replaced, len(error_list)))

file_path = os.path.join(savepath, "data", "kt_pricelist_{}.json".format(timestamp))
save_json(file_path, kt_pricelist)

file_path = os.path.join(savepath, "data", "kt_packages_{}.json".format(timestamp))
save_json(file_path, kt_packages)

file_path = os.path.join(savepath, "data", "kt_prices_{}.json".format(timestamp))
save_json(file_path, kt_prices)

file_path = os.path.join(savepath, "data", "kt_priceerrors_{}.json".format(timestamp))
save_json(file_path, error_list)

data['packages'] = kt_packages
data['pricelist'] = kt_pricelist

save_pickle_data(data, pickle_file)