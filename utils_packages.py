from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_packages(kaptioclient, savepath, packages, tax_profiles, occupancy, channelid, currency="CAD"):
    package_count = 0
    price_count = 0
    error_count = 0
    fixed_count = 0

    kt_1040 = []
    kt_1020 = []
    new_prices = []
    search_values = {
        'tax_profile_id': None,
        'channel_id': channelid,
        'currency': currency,
        'occupancy': None,
        'service_level_ids': None,
        'date_from': None,
        'date_to': None
    }

    for p_value in packages:
        packageid = p_value.get('id')
        package_count += 1
        if not packageid:
            continue

        service_levels = p_value.get('service_levels', {})
        sl_rev = {}
        for s_item in service_levels:
            sl_id = s_item.get('id')
            if sl_id:
                sl_rev[sl_id] = s_item

        # scan and pull the errors...
        for d_key, d_value in p_value.get('pricelist', {}).items():
            if d_key == "errors":
                continue        
            for t_key, t_value in d_value.items():
                if t_key == "errors":
                    continue
                taxprofileid = tax_profiles.get(t_key)

                for o_key, o_value in t_value.items():
                    occ_str = occupancy.get(o_key)

                    for p_item in o_value:
                        price_count += 1
                        new_price_flag = False

                        for e_item in p_item.get('errors', []):
                            error_count += 1
                            e_code = e_item.get('error', {}).get('code') 
                            log_item = {
                                'packageid': packageid,
                                'date': d_key,
                                'tax_profile_id': taxprofileid,
                                'tax_profile': t_key,
                                'occupancy': o_key,
                                'occupancy_id': occ_str,
                                'service_level_id': p_item.get('service_level_id'),
                                'service_level': sl_rev.get(p_item.get('service_level_id'),{}).get('name'),
                                'code': e_code,
                                'message': e_item.get('error', {}).get('message') 
                            }
                            
                            if e_code == 1040:
                                kt_1040.append(log_item)
                                continue

                            # update search_values
                            search_values['tax_profile_id'] = taxprofileid
                            search_values['occupancy'] = occ_str
                            search_values['service_level_ids'] = p_item.get('service_level_id')
                            search_values['date_from'] = d_key
                            search_values['date_to'] = d_key
                            search_values['filter'] ='id=={}'.format(packageid)
                            
                            # we can try to get a new value from the API...
                            new_info = kaptioclient.get_packageprice(
                                    savepath=savepath, 
                                    packageid=packageid,
                                    search_values=search_values
                                    )

                            try:
                                for item in new_info.get('data', []):
                                    if len(item.get('errors')) == 0:
                                        # we need to get the adv serach the get the items here...
                                        log_item['data'] = new_info.get('data', [])
                                        new_prices.append(log_item)
                                        if fixed_count % 100 == 0:
                                            logger.info("{}/{} {} [{}] =>\n\tUpdating price in data {} {} {} {} {}".format(
                                                        fixed_count, error_count, price_count, package_count,
                                                        packageid, d_key, t_key, o_key, p_item.get('service_level_id')))
                                        new_price_flag = True
                                        fixed_count += 1
                                        break
                                
                                if not new_price_flag:
                                    # get the items info...
                                    search_values = {
                                        "tax_profile_id":taxprofileid,  # Required    #Zero
                                        "channel_id":channelid,         # Required    # travel agent
                                        "currency":currency,            # Required
                                        "occupancy":occ_str,            # Required
                                        "service_level_ids":p_item.get('service_level_id'),   
                                        "date":d_key
                                    }

                                    package_details = kaptioclient.get_packageadv(savepath=savepath, 
                                            packageid=packageid,
                                            search_values=search_values
                                        )
                                    log_item['data'] = package_details
                            except Exception as ex:
                                if not 'errors' in log_item:
                                    log_item['errors'] = []
                                log_item['errors'].append({'error': ex})
                            finally:
                                kt_1020.append(log_item)
                                    
    data = {}
    data['counts'] = {
        'packages':package_count,
        'prices': price_count,
        'errors': error_count,
        'fixed': fixed_count
    }
    data['prices'] = new_prices
    data['logs'] = {
        '1040': kt_1040,
        '1020': kt_1020
    }
    return data
