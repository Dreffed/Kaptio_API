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
import path
from time import time
from datetime import datetime
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

def process_allsell(config, data, kt, savepath):
    if not data:
        data = {}

    return data

def process_bulkloader(config, data, kt, savepath):
    if not data:
        data = {}

    config_path = os.path.join(savepath, 'config', 'all_sell.xl.config.json')
    rows = get_packagerows(data.get('packages', []))

    for t_key in data.get('tax_profiles', {}).keys():
        extract_data = get_pricedata(data, rows, t_key)
        data['errors'] = extract_data.get('errors')
        data['price_data'] = extract_data.get('price_data')

        xl_config = load_bulkloaderconfig(config_path)

        generate_bulkloader(
                price_data=extract_data.get('price_data', []), 
                savepath=savepath, 
                template='Rocky Bulk Cost Loader template.xlsx', 
                yearnumber=2020, 
                versionnumber='1.2',
                tax_profile=t_key,
                config=xl_config
            )

    return data

def process_xml(config, data, kt, savepath):
    if not data:
        data = {}

    #xmlconfigpath = "xml_fields.json"
    #xml_fields = load_json(xmlconfigpath)

    contentconfigpath = "content_fields.json"
    content_fields = load_json(contentconfigpath)

    if not data.get('content'):
        raise "not content loaded"
    
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

    generate_xml(
            packages=data.get('packages', []),
            pricelist=data.get('pricelist', {}), 
            content=kt_pcontent,
            departure_types=departure_types, 
            yearnumber=2020, 
            tax_profile='Zero Rated',
            savepath=savepath
        )

    logger.info("Generated XML Files")
    return data