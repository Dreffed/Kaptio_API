# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from kaptiorestpython.ograph import KaptioOGraph
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, copy_pickles
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def load_metadata(config, data, kt, savepath):
    if not data:
        data = {}

    try:
        data['channels'] = kt.get_channels(savepath)
        data['service_levels'] = kt.get_servicelevels(savepath)
        data['languages'] = kt.get_languages(savepath)
        data['client'] = kt.get_client(savepath)

    except Exception as ex:
        if config.get('switches', {}).get('errors'):
            logger.info('=== ERROR: {} => {}'.format(json.dumps(data, indent=2), ex))

    occupancy = {
        "single":"1=1,0",
        "double":"1=2,0",
        "triple":"1=3,0",
        "quad":"1=4,0"
    }

    child_occupancy = {
        "double_child":"1=1,1",
        "triple_1child":"1=2,1",
        "triple_2child":"1=1,2",
        "quad_1child":"1=3,1",
        "quad_2child":"1=2,2",
        "quad_3child":"1=1,3"
    }

    data['occupancy'] = occupancy
    data['occupancy_child'] = child_occupancy

    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA",
        "Foreign":"a8H4F0000003uJbUAI",
        "Domestic":"a8H4F0000003tnfUAA"
    }
    data['tax_profiles'] = tax_profiles

    data['season'] = {
        'start': '2020-04-01',
        'end': '2020-10-31'
    }

    logger.info("loaded metadata")
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} [{}]".format(key, len(value), type(value)))
        else:
            logger.info("\t{} : No Values".format(key))

    return data

def clear_data(config, data, kt, savepath):
    if not data:
        data = {}

    if config.get('flags', {}).get('switches', {}).get('full') or config.get('flags', {}).get('switches', {}).get('reload'):
        logger.info("reloading data...")
        data = {}

    return data

def save_data(config, data, kt, savepath):
    if not data:
        data = {}
    pickle_file = config.get('presets', {}).get('pickle')
    save_pickle_data(data, pickle_file)
    return data

def backup_data(config, data, kt, savepath):
    if not data:
        data = {}
    if not data.get('backup'):
        data['backup'] = {}
        
    data['backup'] = {**data.get('backup',{}), **copy_pickles(savepath)}

    return data

def init_partial(config, data, kt, savepath):
    if not data:
        data = {}

    if not 'packages' in data:
        data['packages'] = []

    for pkg_id in config.get('packages', []):
        logger.info('\t{}'.format(pkg_id))      

        try:
            run_data = kt.get_package(savepath, pkg_id)
            data['packages'].append(run_data)

        except Exception as ex:
            if config.get('flags', {}).get('switches', {}).get('errors'):
                logger.error('=== ERROR: {} => {}'.format(pkg_id, ex))

            data['packages']= {
                'error': ex
            }

    return data

def process_packages(config, data, kt, savepath):
    if not data:
        data = {}

    if not 'packages' in data:
        data['packages'] = {}

    data['packages'] = kt.get_packages(savepath)

    return data

def promote_custom(config, data, kt, savepath):
    if not data:
        data = {}

    logger.info("promoting custom fields...")

    for p_value in data.get('packages', []):
        try:
            p_key = p_value.get('id')
            for c_key, c_value in p_value.get('custom_fields',{}).items():
                if not c_key in p_value:
                    p_value[c_key] = c_value
                    logger.debug("\t{} => {}:{}".format(p_key, c_key, c_value))
        except Exception as ex:
            if config.get('flags', {}).get('switches', {}).get('errors'):
                logger.error('=== ERROR: {}:\n{}\n===\n\t => {}'.format(p_key, p_value, ex))

    return data

def process_dates(config, data, kt, savepath):
    if not data:
        data = {}

    package_field = 'packages'
    key_field = 'package_dates'

    season_start = data.get('season',{}).get('start')
    season_end = data.get('season',{}).get('end')

    logger.info("checking dates...")
    reload = config.get('flags', {}).get('switches', {}).get('reload')
    
    for p_value in data.get(package_field, []):
        if p_value.get(key_field, []):
            if not reload:
                continue

        p_key = p_value.get('id')
        # load in the dates...
        p_value['dates'] = kt.get_packagedepartures(savepath, p_key, season_start, season_end)
        
        logger.debug("\tloaded {} => {} dates".format(p_key, len(p_value.get('dates', []))))
    
    return data

def process_prices(config, data, kt, savepath):
    if not data:
        data = {}
    
    package_field = 'packages'
    key_field = 'package_pricelist'

    logger.info("loading prices...")

    reload = config.get('flags', {}).get('switches', {}).get('reload')

    channelid=config.get("presets", {}).get("channelid")
    currency=config.get("presets", {}).get("currency", "CAD")
    
    use_profiles = data.get('tax_profiles', {})
    tax_profiles = {}
    for tp in config.get("presets", {}).get("tax_profiles"):
        if tp in use_profiles:
            tax_profiles[tp] = use_profiles.get(tp)
    
    occupancy = data.get('occupancy', {})

    for p_value in data.get(package_field, []):
        if p_value.get(key_field, []):
            if not reload:
                continue
        
        if not p_value.get('active'):        
            continue        
        p_key = p_value.get('id')

        dates = []
        for d in p_value.get('dates', []):
            dates.append(d)

        if len(dates) == 0:
            for d in p_value.get('package_departures', []):
                if d.get('active'):
                    dates.append(d.get('date'))
                    
        services = p_value.get('service_levels' ,{})        

        logger.info("\told {} pricelist".format(len(p_value.get(key_field, []))))
        run_data = kt.walk_package(
            savepath=savepath, 
            packageid=p_key, 
            dates=dates, 
            tax_profiles=tax_profiles, 
            occupancy=occupancy, 
            services=services,
            channelid=channelid,
            currency=currency
        )
        if not p_key in data.get('pricelist',{}):
            data['pricelist'][p_key] = {}

        p_value['pricelist'] = run_data
        data['pricelist'][p_key]['pricelist'] = run_data
        logger.info("\tloaded {} dates".format(len(p_value.get(key_field, []))))

    return data

def process_content(config, data, kt, savepath):
    if not data:
        data = {}
    
    kt_packages = data.get('packages', [])
    kt_content = {}
    if 'content' in data:
        del data['content']
        
    kt = get_ograph(config, savepath)

    if not 'content' in data:
        for p in kt_packages:
            # get the content
            if not p['id'] in kt_content:
                kt_content[p['id']] = kt.get_content(p['id'])
        data['content'] = kt_content

    logger.info("Found content for {}".format(len(kt_content)))
    return data

def process_items(config, data, kt, savepath):
    if not data:
        data = {}

    return data

def get_ograph(config, savepath):
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
    return kt

def get_ktapi(config, savepath):
    kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])
    return kt