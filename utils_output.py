# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import save_json, load_json, scanfiles
from utils_packages import process_packages
from utils_extractors import get_packagerows, get_pricedata
from utils_excel import generate_bulkloader, load_WB, load_bulkloaderconfig
from utils_dict import extract_rows
from utils_xml import generate_xml
import json
import pickle
import os

from time import time
from datetime import datetime
import csv
import logging

logger = logging.getLogger(__name__)

def process_errors(config, data, kt, savepath):
    if not data:
        data = {}
    
    channelid = config.get('presets', {}).get('channelid')
    
    use_profiles = data.get('tax_profiles', {})
    tax_profiles = {}
    for tp in config.get("presets", {}).get("tax_profiles"):
        if tp in use_profiles:
            tax_profiles[tp] = use_profiles.get(tp)
    
    occupancy = data.get('occupancy')
    kt_packages = data.get('packages')
    
    kt_updates = process_packages(kaptioclient=kt, 
            savepath=savepath, packages=kt_packages, 
            tax_profiles=tax_profiles, channelid=channelid, 
            occupancy=occupancy
        )
    data['updates'] = kt_updates
    return data

def remove_pricedata(config, data, kt, savepath):
    if not data:
        data = {}

    if data.get('price_data'):
        del data['price_data']

    if data.get('errors'):
        del data['errors']

    return data

def process_pricedata(config, data, kt, savepath):
    if not data:
        data = {}

    rows = get_packagerows(data.get('packages', []))
    file_version=config.get("presets", {}).get("version", "0.1")
    currency=config.get("presets", {}).get("currency", "CAD")
    season_date = config.get('season', {}).get('start')
    season_year = season_date[:4]
    
    for t_key in data.get('tax_profiles', {}).keys():
        if not data.get('price_data',{}).get(t_key):
            extract_data = get_pricedata(data, rows, t_key)
            if not 'errors' in data:
                data['errors'] = {}
            if not 'price_data' in data:
                data['price_data'] = {}

            data['errors'][t_key] = extract_data.get('errors')
            data['price_data'][t_key] = extract_data.get('price_data')

        file_name = "allsell_{}_{}_{}.{}.csv".format(season_year, currency, t_key, file_version)

        price_rows = data['price_data'][t_key]
        if len(price_rows) > 0:
            fieldnames = data['price_data'][t_key][0].keys()
            logger.info('Saveing Data : {}'.format(file_name))
            
            with open(file_name, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(price_rows)

    return data

def process_bulkloader(config, data, kt, savepath):
    if not data:
        data = {}

    config_path = os.path.join(savepath, 'config', 'all_sell.xl.config.json')
    rows = get_packagerows(data.get('packages', []))
    file_version=config.get("presets", {}).get("version", "0.1")
    currency=config.get("presets", {}).get("currency", "CAD")
    season_date = config.get('season', {}).get('start')
    season_year = season_date[:4]

    for t_key in data.get('tax_profiles', {}).keys():
        if not data.get('price_data',{}).get(t_key):
            extract_data = get_pricedata(data, rows, t_key)
            if not 'errors' in data:
                data['errors'] = {}
            if not 'price_data' in data:
                data['price_data'] = {}

            data['errors'][t_key] = extract_data.get('errors')
            data['price_data'][t_key] = extract_data.get('price_data')

        xl_config = load_bulkloaderconfig(config_path)

        generate_bulkloader(
                price_data=data.get('price_data', {}).get(t_key,[]), 
                data=data,
                savepath=savepath, 
                template=None, 
                yearnumber=season_year, 
                versionnumber=file_version,
                tax_profile=t_key,
                config=xl_config,
                currency=currency
            )

    return data

def process_xml(config, data, kt, savepath):
    if not data:
        data = {}

    #xmlconfigpath = "xml_fields.json"
    #xml_fields = load_json(xmlconfigpath)
    currency=config.get("presets", {}).get("currency", "CAD")
    season_date = config.get('season', {}).get('start')
    season_year = season_date[:4]

    contentconfigpath = "content_fields.json"
    content_fields = load_json(contentconfigpath)

    if not data.get('content'):
        logger.error("No content has been loaded")
        return data
    
    # pivot the content into a cleaner format....
    kt_pcontent = {}
    for key, value in data.get('content', {}).items():

        if isinstance(value, list):
            if not key in kt_pcontent:
                kt_pcontent[key] = []

            for item in value:
                row = extract_rows(item, content_fields) 
                kt_pcontent[key].append(row)

    logger.info("Content {}".format(len(data.get('content', {}))))
    logger.info("Pivot {}".format(len(kt_pcontent)))    

    departure_types = [
            'Anyday', 'Seasonal', 'Fixed'
        ] 
    kt_packages = data.get('packagefull',{})
    kt_pricelist = data.get('pricelist', {})

    for t_key in data.get('tax_profiles', {}).keys():
        logger.info("generating XML for {} {}\n\tPackages: {}\n\tPricelists: {}".format(t_key, currency, len(kt_packages), len(kt_pricelist)))

        generate_xml(
                packages=kt_packages,
                pricelist=kt_pricelist, 
                content=kt_pcontent,
                departure_types=departure_types, 
                yearnumber=season_year, 
                tax_profile=t_key,
                savepath=savepath,
                currency=currency
            )

    logger.info("Generated XML Files")
    return data