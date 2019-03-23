from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
import json
import pickle
import os
import path
from time import time
from datetime import datetime

def display_data(pickle_file, data, name):
    print("{}: {}".format(name, pickle_file))
    for key, value in data.items():
        print("\t{} => {}".format(key, len(value)))

update_lookups = False

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

print("Local Pickles:")
for f in scanfiles('.', r".*\.pickle"):
    print("\t{} => {}".format(f['file'], f['folder']))

print("Remote Pickles:")
for f in scanfiles(os.path.join(savepath, 'config'), r".*\.pickle"):
    print("\t{} => {}".format(f['file'], f['folder']))

pickle_file = "kaptio_allsell.pickle"

data = get_pickle_data(pickle_file)

display_data(pickle_file, data, "Existing")

source_pickle_file = os.path.join(savepath, "config", "kaptio_allsell.Parandarus.201903230942.pickle")
data_src = get_pickle_data(source_pickle_file)

display_data(source_pickle_file, data_src, "Remote")

if 'content' in data_src:
    print("Fetching remote cached content")
    kt_content = data_src.get('content')

    if kt_content:
        data['content'] = kt_content
        print("Retrieved remote cached content")

        save_pickle_data(data, pickle_file)

display_data(pickle_file, data, "Updated")
