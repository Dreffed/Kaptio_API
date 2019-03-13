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

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

for key, value in data.items():
    print(key)

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

kt_packages = []
if 'packages' in data:
    kt_packages = data['packages']

print("Loaded {} packages".format(len(kt_packages)))

for p in kt_packages:
    if 'pricelist' in p:
        if 'errors' in p['pricelist']:
            print("{} has pricelist".format(p['id']))
            print("\tERROR Found: {}".format(p['pricelist']['errors']))
