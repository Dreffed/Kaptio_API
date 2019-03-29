import json
import pickle
from datetime import datetime
import logging
from utils import get_pickle_data, save_pickle_data, save_json, load_json, scanfiles, scan_packagefiles

base_name = "kaptio_allsell"
pickle_file = "{}.pickle".format(base_name)
data = get_pickle_data(pickle_file)

save_json("{}.json".format(base_name), data)