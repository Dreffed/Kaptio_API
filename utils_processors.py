# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
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
        data['packages'] = {}

    for pkg_id in config.get('packages', []):
        logger.info('\t{}'.format(pkg_id))      

        try:
            data['packages'][pkg_id] = kt.get_package(savepath, pkg_id)
        except Exception as ex:
            if config.get('flags', {}).get('switches', {}).get('errors'):
                logger.error('=== ERROR: {} => {}'.format(pkg_id, ex))

            data['packages'][pkg_id] = {
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

    for p_key, p_value in data.get('packages', {}).items():
        try:
            for c_key, c_value in p_value.get('custom_fields',{}).items():
                if not c_key in p_value:
                    data['packages'][p_key][c_key] = c_value
                    logger.info("\t{} => {}:{}".format(p_key, c_key, c_value))
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
    
    for p_key, p_value in data.get(package_field, {}).items():
        if p_value.get(key_field, []):
            if not reload:
                continue

        # load in the dates...
        logger.info("\told {} dates".format(len(data.get(package_field, {}).get(p_key,{}).get(key_field, []))))

        data[package_field][p_key][key_field] = kt.get_packagedepartures(savepath, p_key, season_start, season_end)
        
        logger.info("\tloaded {} dates".format(len(data.get(package_field, {}).get(p_key,{}).get(key_field, []))))
    
    return data

def process_prices(config, data, kt, savepath):
    if not data:
        data = {}
    
    package_field = 'packages'
    key_field = 'package_pricelist'
    log = config.get('flags', {}).get('switches', {}).get('logging')
    debug = config.get('flags', {}).get('switches', {}).get('debug')

    logger.info("loading prices...")

    reload = config.get('flags', {}).get('switches', {}).get('reload')
    for p_key, p_value in data.get(package_field, {}).items():
        if p_value.get(key_field, []):
            if not reload:
                continue

        dates = []
        for d in p_value.get('package_dates', []):
            dates.append(d)

        if len(dates) == 0:
            for d in p_value.get('package_departures', []):
                if d.get('active'):
                    dates.append(d.get('date'))

        tax_profiles = data.get('tax_profiles', {})
        occupancy = data.get('occupancy', {})
        services = p_value.get('service_levels' ,{})        

        logger.info("\told {} pricelist".format(len(data.get(package_field, {}).get(p_key,{}).get(key_field, []))))

        data[package_field][p_key][key_field] = kt.walk_package(
            savepath=savepath, 
            packageid=p_key, 
            dates=dates, 
            tax_profiles=tax_profiles, 
            occupancy=occupancy, 
            services=services, 
            logger=log,
            debug=debug
        )
        logger.info("\tloaded {} dates".format(len(data.get(package_field, {}).get(p_key,{}).get(key_field, []))))

    return data