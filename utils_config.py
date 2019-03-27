import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file):
    config = {}

    if not os.path.exists(config_file):
        settings = {}

        settings['packages'] = [
            'a754F0000000B0aQAE',
            'a754F0000000B0QQAU',
            'a754F0000000B3oQAE',
            'a754F0000000B3tQAE',
            'a754F0000000B0pQAE'
        ]

        settings['flags'] = {
            'parts':[
                'packages',
                'dates',
                'prices',
                'errors',
                'content',
                'items',
                'allsell',
                'bulkloader',
                'xml'
            ],
            'reload':[
                'packages',
                'dates',
                'prices'
            ],
            'switches': {
                'logging':True,
                'reload':True,
                'debug': True,
                'scan_local':False,
                'scan_remote': False,
                'check_cache': False,
                'update_system': True,
                'import_remote': True,
                'backup_pickle': True
            }
        }

        settings['regex'] = {
            'pickle': {
                'folders':['LOCAL'],
                're':r'kaptio_api(?P<process>[a-zA-Z0-9_-])\.pickle'
            },
            'archive':{
                'folders': ['LOCAL', 'KTAPICONFIG'],
                're':r'kaptio_api(?P<process>_[a-zA-Z0-9-]*?)(?P<machine>_[a-zA-Z0-9_-]+?)(?P<timestamp>_[\d]{8,18}?)\.pickle'
            }
        }

        settings['configurations'] = {
            'kaptio':{
                'folder':'KTAPICONFIG',
                'name':'kaptio_config.json'
            },
            'pickle':{
                'folder':'LOCAL',
                'name':'kt_api_data.pickle'
            }
        }

        settings['folders'] = {
            "KTAPIDATA": {
                "name":"Kaptio API Data",
                "pathtype": "REL",
                "rootpath":"HOME",
                "basefolders":["OneDrive - Great Canadian Railtour Co", "Jupyter_NB", "data"],
                "regex":""
            },
            "KTAPICONFIG": {
                "name":"Kaptio API Config",
                "pathtype": "REL",
                "rootpath":"HOME",
                "basefolders":["OneDrive - Great Canadian Railtour Co", "Jupyter_NB", "config"],
                "regex":""
            },
            "KTAPI": {
                "name":"Kaptio API Tool",
                "pathtype": "REL",
                "rootpath":"HOME",
                "basefolders":["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"],
                "regex":""
            },
            '_locations':{
                '_data':'KTAPIDATA',
                '_config':'KTAPICONFIG',
                '_remote': 'KTAPI',
                '_local':'LOCAL',
                '_save':'KTAPIDATA'
            }
        }

        with open(config_file, "w") as f:
            json.dump(settings, f, indent=4)
        
    # load this into
    with open(config_file, "r") as f:  
        config = json.load(f)
    return config

def get_folderpath(config, foldertype, paths):
    if foldertype in config.get('folders', {}).get('_locations',{}):
        path_loc = config.get('folders', {}).get('_locations', {}).get(foldertype, 'LOCAL')
        path_values = config.get('folders', {}).get(path_loc, {})
    elif foldertype in config.get('folders', {}):
        path_values = config.get('folders', {}).get(foldertype, {})
    else:
        path_values = None

    if path_values:
        # get the path specified...
        base_type = path_values.get('pathtype', 'REL')
        base_path = path_values.get('rootpath')
        base_folders = path_values.get('basefolders')
        
        if base_type == 'REL':
            if base_path in paths:
                root_folder = paths.get(base_path)
            elif base_path in config.get('folders', {}):
                root_folder = get_folderpath(config, base_path, paths)
            else:
                root_folder = None
            return os.path.join(root_folder, *base_folders)
        else:
            return base_path
    else:
        return paths.get('LOCAL')

def get_configuration_path(config, name, paths):
    _setup = config.get('configurations',{}).get(name)
    _config_path = get_folderpath(config, _setup.get('folder'), paths)
    if config.get('flags', {}).get('switches', {}).get('logging'):
        logger.info('Configpath: {}'.format(_config_path))      
    return os.path.join(_config_path, _setup.get('name'))