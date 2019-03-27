from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

update_lookups = False

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

pickle_files = scanfiles('.', r".*\.pickle")
logger.info("Pickles:")
for f in pickle_files:
    logger.info("\t{} => {} [{}]".format(f['file'], f['folder'], json.dumps(f, indent=2)))

pickle_file = "kaptio_allsell.pickle"

data = get_pickle_data(pickle_file)

for key, value in data.items():
    logger.info(key)

if update_lookups:
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

    data['tax_profiles'] = tax_profiles
    data['occupancy'] = occupancy

    save_pickle_data(data, pickle_file)

#"": {"_output": ""},
#
contentconfigpath = "content_fields.json"
fields = load_json(contentconfigpath)

kt_packages = []
if 'packages' in data:
    kt_packages = data['packages']

logger.info("Loaded {} packages".format(len(kt_packages)))


logger.info("Error report ")
error_logs = []
for p in kt_packages:
    p_row = {}
    p_row['packageid'] = p.get('id')
    p_row['packagename'] = p.get('external_name')
    p_row['packagerecordtype'] = p.get('record_type_name')
    p_row['packagelength'] = p.get('length')
    p_row['packagedatecount'] = len(p.get('dates', {}))

    for d_key, d_value in p.get('pricelist').items():
        if d_key == 'errors':
            continue
        for t_key, t_value in d_value.items():
            for o_key, o_value in t_value.items():
                pass
                
    if 'pricelist' in p:
        if 'errors' in p['pricelist']:
            logger.info("{} has pricelist".format(p['id']))
            logger.info("\tERROR Found: {}".format(p['pricelist']['errors']))

if 'content' in data:
    kt_content = data['content']

rows = []
for key, value in kt_content.items():
    if isinstance(value, list):
        for item in value:
            row = extract_rows(item, fields) 
            rows.append(row)

logger.info(len(rows))

file_path = os.path.join(savepath, "data", "kt_contents_{}.json".format(timestamp))
save_json(file_path, kt_content)

file_path = os.path.join(savepath, "data", "kt_content_rows_{}.json".format(timestamp))
save_json(file_path, rows)
