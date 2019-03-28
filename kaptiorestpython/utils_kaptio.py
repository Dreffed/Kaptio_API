from time import time, sleep
import json
import os
import requests
from datetime import datetime
from kaptiorestpython.helper.http_lib import HttpLib
from kaptiorestpython.helper.exceptions import APIException
from utils import save_json, scan_packagefiles
import multiprocessing
from multiprocessing.queues import Empty
import logging.config

logger = logging.getLogger(__name__)

def has_empty_warning(result):
    if 'result' not in result \
        and 'warnings' in result \
        and len(result['warnings']) \
        and result['warnings'][0] == 'No assets found for the given search criteria.':
        return True

    return False

def load_kaptioconfig(kaptio_config_file):
    # load the kaptio config data
    kaptio_config = {}

    if not os.path.exists(kaptio_config_file):
        settings = {}

        settings['api'] = {}
        settings['api']['baseurl'] = "kaptio-staging.herokuapp.com"

        settings['api']['auth'] = {}
        settings['api']['auth']['key'] = "<KEY>"
        settings['api']['auth']['secret'] = "<SECRET>"

        with open(kaptio_config_file, "w") as f:
            json.dump(settings, f, indent=4)
        
    # load this into
    with open(kaptio_config_file, "r") as f:  
        kaptio_config = json.load(f)
    return kaptio_config

def display_fields(data):
    for key, value in data.items():
        if isinstance(value, dict):
            logger.info("{} -> DICT".format(key))
        elif isinstance(value, list):
            logger.info("{} -> LIST {}".format(key, len(value)))
        else:
            logger.info("{} -> {}".format(key, value))
