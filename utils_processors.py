# load the dependancies
from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from kaptiorestpython.ograph import KaptioOGraph
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, copy_pickles
from utils_config import get_folderpath, load_config, get_configuration_path
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
        logger.error('{} => {}'.format(json.dumps(data, indent=2), ex))

    data['season'] = config.get("season", {})
    data['occupancy'] = config.get("occupancy", {})
    data['occupancy_child'] = config.get("child_occupancy", {}) 
    data['tax_profiles'] = config.get("tax_profiles", {})

    logger.info("loaded metadata")
    for key, value in data.items():
        if value:
            logger.info("\t{} => {} [{}]".format(key, len(value), type(value)))
        else:
            logger.info("\t{} : No Values".format(key))

    return data

def update_taxprofiles(config, data, kt, savepath):
    q = "SELECT ID, sfxId__c, Name, currencyIsoCode FROM KaptioTravel__TaxProfile__c"

    kt = get_ograph(config, savepath)
    resp = kt.process_query(q)

    data['tax_profiles'] = config.get("tax_profiles", {})
    for r in resp.get('records', []):
        for t_key, t_value in config.get("tax_profiles", {}):
            t_name_new = r.get('Name')
            t_key_new = r.get('Id')
            if t_name_new.lower().startswith(t_key.lower()):
                if t_key_new != t_value:
                    logger.info("Updating tax profile id: {}: {} => {}".format(t_key, t_value, r.get('Id')))
                    data['tax_profiles'][t_key] = r.get('Id')

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

    channelid=None
    for c in data.get("channels",[]):
        if c.get("id") == config.get("presets", {}).get("channelid") or \
                c.get("name") == config.get("presets", {}).get("channelname") or \
                c.get("code") == config.get("presets", {}).get("channelcode"):
            logger.info("Matched channeldata {} => {}".format(c.get("name"), c.get("id")))
            channelid = c.get("id")
            break
        
    if not channelid:
        logger.error("Failed to match channelid {}".format(config.get("presets", {}).get("channelid") ))
        raise Exception("Failed to match channelid {}".format(config.get("presets", {}).get("channelid")))
        
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
        run_data = kt.process_package_prices(
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

def augment_pricelists(config, data, kt, savepath):
    if not data:
        data = {}
    prices = {}

    # occupancy, tax profiles, service_levels
    occupancy = data.get('occupancy', {})
    currency=config.get("presets", {}).get("currency", "CAD")
    
    use_profiles = data.get('tax_profiles', {})
    tax_profiles = {}
    for tp in config.get("presets", {}).get("tax_profiles"):
        if tp in use_profiles:
            tax_profiles[tp] = use_profiles.get(tp)

    # scan the data and load in other records...
    for p_value in data.get('packages', []):
        p_key = p_value.get("id")
        p_list = data.get("pricelist", {}).get(p_key, {}).get("pricelist")
        if p_list:
            p_value['pricelist'] = p_list

        i_data = {}

        dates = []
        for d in p_value.get('dates', []):
            dates.append(d)

        if len(dates) == 0:
            for d in p_value.get('package_departures', []):
                if d.get('active'):
                    dates.append(d.get('date'))

        service_levels = {}
        for s_item in p_value.get('service_levels',[]):
            sid = s_item.get('id')
            sname = s_item.get('name')
            if s_item.get('active'):
                service_levels[sid] = {'name': sname, 'active': s_item.get('active')}        

        for d_key in dates:
            i_data[d_key] = {}
            for t_key, t_value in tax_profiles.items():
                i_data[d_key][t_key] = {}
                i_data[d_key][t_key]["_id"] = t_value
                for o_key, o_value in occupancy.items():
                    i_data[d_key][t_key][o_key] = {}
                    i_data[d_key][t_key][o_key]["_id"] = o_value
                    for s_key, s_value in service_levels.items():
                        i_data[d_key][t_key][o_key][s_key] = {}
                        i_data[d_key][t_key][o_key][s_key]["_data"] = s_value

                        # save for the current currency....
                        i_data[d_key][t_key][o_key][s_key][currency] = {}
                        i_data[d_key][t_key][o_key][s_key][currency]['date'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
                        if p_list:
                            for price_item in p_list.get(d_key, {}).get(t_key, {}).get(o_key, []):
                                if price_item.get('service_level_id') == s_key:
                                    i_data[d_key][t_key][o_key][s_key][currency]['total_price'] = price_item.get('total_price')
                                    if len(price_item.get('errors',[])) > 0:
                                        i_data[d_key][t_key][o_key][s_key][currency]['errors'] = price_item.get('errors')

        if prices.get(p_key):
            prices[p_key] = {**prices[p_key], **i_data}
        else:
            prices[p_key] = i_data
            
    data['prices'] = prices
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
    config_type = config.get("configurations", {}).get("run", {}).get("kaptio")
    kaptio_config_file = get_configuration_path(config, config_type, config.get('paths', []))
    kaptio_config = load_kaptioconfig(kaptio_config_file)    
    
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
    config_type = config.get("configurations", {}).get("run", {}).get("kaptio")
    kaptio_config_file = get_configuration_path(config, config_type, config.get('paths', []))
    kaptio_config = load_kaptioconfig(kaptio_config_file)    
    baseurl = kaptio_config['api']['baseurl']

    kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])
    return kt