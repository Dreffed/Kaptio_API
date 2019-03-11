# load the dependancies
import json
import pickle
import os
import path
import pandas as pd
import requests
from time import time
from datetime import datetime
from multiprocessing import Lock, Process, Queue, current_process
import queue

datapaths = ["C:/", "Users", "David Gloyn-Cox", "OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(*datapaths)
print(savepath)

# helper functions
def get_pickle_data(pickleName):
    data = {}
    if os.path.exists(pickleName):
        print('Loading Saved Data... [%s]' % pickleName)
        with open(pickleName, 'rb') as handle:
            data = pickle.load(handle)
    return data

def save_pickle_data(data, pickleName):
    print('Saving Data... [%s]' % pickleName)
    with open(pickleName, 'wb') as handle:
        pickle.dump(data, handle)
        
def save_json(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def save_response(savepath, base_name, resp, field_name):
    json_data = json.loads(resp.text)
    suffix_name = field_name
    if field_name in json_data:
        suffix_name = json_data[field_name]
    
    json_path = os.path.join(savepath, "data", "{}_{}.json".format(base_name,suffix_name))
    save_json(json_path, json_data)
    return json_data

def get_responsedata(resp):
    json_data = json.loads(resp.text)
    return json_data

def display_fields(data):
    for key, value in data.items():
        if isinstance(value, dict):
            print("{} -> DICT".format(key))
        elif isinstance(value, list):
            print("{} -> LIST {}".format(key, len(value)))
        else:
            print("{} -> {}".format(key, value))

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


def api_data(headers, baseurl, url_data, paramstr ="", querystr = "", body = None):
    thisurl = 'http://{}/{}/{}{}{}'.format(baseurl, url_data['version'], url_data['suburl'], paramstr, querystr)
    #print("{}:{}\n\t{}". format(url_data["name"], url_data['method'], thisurl))

    if url_data['method'] == "GET":
        r = requests.get(thisurl, headers=headers)
    elif url_data['method'] == "POST":
        r = requests.post(thisurl, headers=headers, json=body)
        
    return r

def api_list(headers, baseurl, url_data, paramstr, querystr, body = None):
    thisurl = 'http://{}/{}/{}{}{}'.format(baseurl, url_data['version'], url_data['suburl'], paramstr, querystr)
    #print("{}:{}\n\t{}". format(url_data["name"], url_data['method'], thisurl))
    data = []
    while True:
        if url_data['method'] == "GET":
            r = requests.get(thisurl, headers=headers)
        elif url_data['method'] == "POST":
            r = requests.post(thisurl, headers=headers, json=body)
        
        if r.status_code != 200:
            print('ERROR: {} => {}'.format(r, r.text))
            break
        
        json_data = json.loads(r.text)
        print('====')
        display_fields(json_data)
        data.extend(json_data['records'])
        
        if json_data['next']:
            thisurl = json_data['next']
        else:
            break

    return data

def get_channels(headers, savepath, baseurl):
    url_data = {}
    url_data['name'] = "Channels"
    url_data['version'] = 'v2.0'
    url_data['suburl'] = 'channels'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    data = api_list(headers, baseurl, url_data, paramstr, querystr)
    print("found: {}".format(len(data)))
    file_path = os.path.join(savepath, "data", "kt_channels.json")
    save_json(file_path, data)
    return data

def get_client(headers, savepath, baseurl):
    url_data = {}
    url_data['name'] = "Client"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'client/config'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, 'kt', r, 'client')
        display_fields(data)
    else:
        print("Failed: {} => {}".format(r, r.text))    
    return data

def get_package(headers, savepath, baseurl):
    url_data = {}
    url_data['name'] = "Package"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'packages'
    url_data['method'] = 'GET'
    paramstr = '/{}'.format(packageid)
    querystr = ''

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, url_data['name'], r, 'id')
        display_fields(data)
    else:
        print("Failed: {} => {}".format(r, r.text))
    return data

def get_packages(headers, savepath, baseurl):
    # build out all the packages
    url_data = {}
    url_data['name'] = "All packages"
    url_data['version'] = 'v2.0'
    url_data['suburl'] = 'packages'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    data = api_list(headers, baseurl, url_data, paramstr, querystr)
    print("found: {}".format(len(data)))
    file_path = os.path.join(savepath, "data", "kt_packages.json")
    save_json(file_path, data)
    return data
    
def get_search(headers, savepath, baseurl, packageid, search_values):
    # search for a range of packages
    save_str = ''
    search_list = []
    for key, value in search_values.items():
        if len(value) > 0:
            search_list.append("{}={}".format(key, value))
                
    if len(search_list) > 0:
        querystr = "?{}".format("&".join(search_list))    
        
    save_str="{}.{}.{}{}".format(packageid,search_values['date_from'],search_values['date_to'],search_values['mode'])

    url_data = {}
    url_data['name'] = "Package Search"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'packages/search'
    url_data['method'] = 'GET'
    paramstr = '' #'/{}/search'.format(packageid)

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, url_data['name'], r, save_str)
        display_fields(data)
    else:
        print("Failed: {} => {}".format(r, r.text))    
    return data

def get_packageadv(headers, savepath, baseurl, packageid, search_values):
    search_list = []
    querystr = ''
    for key, value in search_values.items():
        if len(value) > 0:
            search_list.append("{}={}".format(key, value))
                
    if len(search_list) > 0:
        querystr = "?{}".format("&".join(search_list))        

    url_data = {}
    url_data['name'] = "Package Search"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'packages'
    url_data['method'] = 'GET'
    paramstr = '/{}/advanced'.format(packageid)

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, url_data['name'], r, packageid)
        display_fields(data)
    else:
        print("Failed: {} => {}".format(r, r.text))    
    return data

def get_packagedepartures(headers, savepath, baseurl, packageid, datefrom, dateto):
    url_data = {}
    url_data['name'] = "Package Search"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'packages'
    url_data['method'] = 'GET'
    paramstr = '/{}/departures'.format(packageid)
    search_list = []
    if len(datefrom) > 0:
        search_list.append("{}={}".format("date_from", datefrom))
    if len(dateto) > 0:
        search_list.append("{}={}".format("date_to", dateto))
    
    querystr = ''
    if len(search_list) > 0:
        querystr = "?{}".format("&".join(search_list))  

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, url_data['name'], r, packageid)
        print("Found {} dates".format(len(data)))
    else:
        print("Failed: {} => {}".format(r, r.text))    
    return data


def get_item(headers, savepath, baseurl, itemid):
    url_data = {}
    url_data['name'] = "Item"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'items'
    url_data['method'] = 'GET'
    paramstr = '/{}'.format(itemid)
    querystr = ''

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        data = save_response(savepath, url_data['name'], r, 'id')
        display_fields(data)
    else:
        print("Failed: {} => {}".format(r, r.text))

    return data

def get_items(headers, savepath, baseurl):
    # get all the items
    url_data = {}
    url_data['name'] = "All items"
    url_data['version'] = 'v2.0'
    url_data['suburl'] = 'items'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    data = api_list(headers, baseurl, url_data, paramstr, querystr)
    print("found: {}".format(len(data)))
    file_path = os.path.join(data, "data", "kt_items.json")
    save_json(file_path, data)
    return data

def get_servicelevels(headers, savepath, baseurl):
    # get all the servicelevels
    url_data = {}
    url_data['name'] = "All Servicelevels"
    url_data['version'] = 'v2.0'
    url_data['suburl'] = 'service_levels'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    data = api_list(headers, baseurl, url_data, paramstr, querystr)
    print("found: {}".format(len(data)))
    file_path = os.path.join(savepath, "data", "kt_servicelevels.json")
    save_json(file_path, data)

def get_langauages(headers, savepath, baseurl):
    # get all the languages
    url_data = {}
    url_data['name'] = "All Languages"
    url_data['version'] = 'v2.0'
    url_data['suburl'] = 'languages'
    url_data['method'] = 'GET'
    paramstr = ''
    querystr = ''

    data = api_list(headers, baseurl, url_data, paramstr, querystr)
    print("found: {}".format(len(data)))
    file_path = os.path.join(savepath, "data", "kt_lauguages.json")
    save_json(file_path, data)

def get_packageprice(headers, savepath, baseurl, packageid, date_from, date_to, taxprofileid = 'a8H4F0000003tsfUAA', channelid = 'a6H4F0000000DkMUAU', 
                    occupancy = '1=1,0', services = 'a7r4F0000000AloQAE', debug=False):
    data = []
    errors = []
    search_values = {
        "tax_profile_id":taxprofileid,  # Required    #Zero
        "channel_id":channelid,         # Required    # travel agent
        "currency":"CAD",               # Required
        "occupancy":occupancy,          # Required
        "service_level_ids":services,   
        "date_from":date_from,
        "date_to":date_to,
        "mode":"",
        "filter":"id=={}".format(packageid)
    }

    save_str = ''
    search_list = []
    for key, value in search_values.items():
        if len(value) > 0:
            search_list.append("{}={}".format(key, value))

    if len(search_list) > 0:
        querystr = "?{}".format("&".join(search_list))    
    
    url_data = {}
    url_data['name'] = "Package Search"
    url_data['version'] = 'v1.0'
    url_data['suburl'] = 'packages/search'
    url_data['method'] = 'GET'
    paramstr = ''

    r = api_data(headers, baseurl, url_data, paramstr, querystr)
    if r.status_code == 200:
        rd = get_responsedata(r)
    else:
        print("Failed: {} => {}".format(r, r.text)) 
        return data

    for p in rd['results']:
        for d in p['prices_by_service_level']:
            if len(d['errors']) > 0:
                errors.append(d)
            else:
                data.append(d)
    if debug:
        filepath = os.path.join(savepath, "data", "fn_get_packageprice.json")
        json_msg = {
            "packageid":packageid,
            "query": querystr,
            "resp": rd
        }

        with open(filepath, 'a') as f:
            json.dump(json_msg, f, indent=4)
        
    return {'data':data, 'errors':errors}

def walk_package(headers, savepath, baseurl, packageid, dates, tax_profiles, occupancy, services, debug=False):
    """
    if not isinstance(dates, list):
        raise "[dates] should be a list of date strings 'YYYY-mm-dd'"
    if not isinstance(tax_profiles, dict):
        raise "[tax_profiles] should be a dict{'tax profile name':'SFId'}"
    if not isinstance(occupancy, dict):
        raise "[occupancy] should be a dict{'occupancy name':'filter string'}"
    if not isinstance(services, dict):
        raise "[services] should be a list of dict{'name':'name', 'id':'SFId', 'active':bool}"
    """
    c_count = 0
    # pivot the services to get the name from id:
    service_dict = {}
    service_list = []
    for s in services:
        if s['active']: 
            service_list.append(s['id'])
            service_dict[s['id']] = s
    services_str = ",".join(service_list)
    
    data = {}
    for d in dates:
        data[d] = {}
        for t_key, t_value in tax_profiles.items():
            data[d][t_key] = {}
            for o_key, o_value in occupancy.items():
                pricelist = get_packageprice(headers, savepath, baseurl, packageid, date_from=d, date_to=d, 
                                              taxprofileid=t_value, occupancy=o_value,
                                              services=services_str, debug=debug)
                data[d][t_key][o_key] = []
                if len(pricelist['data']) > 0:
                    data[d][t_key][o_key].extend(pricelist['data'])
                if len(pricelist['errors']) > 0:
                    data[d][t_key][o_key].extend(pricelist['errors'])
                    if not 'errors' in data:
                        data['errors'] = 0
                    data['errors'] += len(pricelist['errors'])
                c_count += 1
        break

    print("\tCalls:{}".format(c_count))
    return data

def process_package_worker(headers, savepath, baseurl, toprocess, tax_profiles, occupancy, processed, debug=False):
    # build a list of Packages
    s_count = 0
    s_time = time()
    
    while True:
        try:
            package = toprocess.get_nowait()
        except queue.Empty:
            break
            
        if package['record_type_name'] == 'Package' and package['active']:
            if package['name'].lower().startswith('test'):
                print("Found test package: {} -> {}".format(package['id'], package['name']))
                return

            s_count += 1
            file_path = os.path.join(savepath, "data", "kt_packages_{}.json".format(package['id']))

            package['services'] = ""
            package['direction'] = ""
            package['product_code'] = ""

            if 'service_levels' in package:
                package['services'] = [s['id'] for s in package['service_levels']]
            if 'custom_fields' in package:
                if 'direction' in package['custom_fields']:
                    package['direction'] = package['custom_fields']['direction']
                if 'product_code' in p['custom_fields']:
                    package['product_code'] = package['custom_fields']['product_code']

            package['pricelist'] = walk_package(headers, savepath, baseurl, package['id'], 
                            dates=package['dates'], tax_profiles=tax_profiles, occupancy=occupancy, 
                            services=package['service_levels'], debug=True)

            processed.put(package)
            save_json(file_path, package)        

def process_packages_pool(headers, savepath, baseurl, packages, worker_count = 5, limit=0, debug = True):
    worker_count = 5

    p_toprocess = Queue()
    p_processed = Queue()
    workers = []

    count = 0
    for p in packages:
        count += 1
        p_toprocess.put(p)
        if limit > 0 and count > limit:
            break
    
    for w in range(worker_count):
        try:
            worker = Process(target=process_package_worker, args=(headers, savepath, baseurl, p_toprocess, tax_profiles, occupancy, p_processed, debug))
        except Exception as ex:
            print(ex)
            
    for w in workers:
        w.join()
        
    kt_processed = []
    while not p_processed.empty():
        kt_processed.append(p_processed.get())

    print(len(kt_processed))
    return kt_processed

def get_extract(headers, savepath, baseurl, packages, tax_profiles, occupancy, debug=False):
    p_count = 0
    s_count = 0
    l_count = 0
    e_count = 0
    s_time = time()
    print("{}:{} => {} {} [{}]".format(p_count, s_count, l_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0))
    
    data = {}
    for p in packages:
        p_count += 1
        if p['record_type_name'] == 'Package' and p['active']:
            if p['name'].lower().startswith('test'):
                continue
            s_count += 1
            file_path = os.path.join(savepath, "data", "kt_package_{}.json".format(p['id']))

            p['services'] = ""
            p['direction'] = ""
            p['product_code'] = ""
            
            if 'service_levels' in p:
                p['services'] = [s['id'] for s in p['service_levels']]
            if 'custom_fields' in p:
                if 'direction' in p['custom_fields']:
                    p['direction'] = p['custom_fields']['direction']
                if 'product_code' in p['custom_fields']:
                    p['product_code'] = p['custom_fields']['product_code']

            if not 'pricelist' in p:
                p['pricelist'] = walk_package(headers, savepath, baseurl, p['id'], dates=p['dates'], tax_profiles=tax_profiles, occupancy=occupancy, services=p['service_levels'], debug=True)

            e_time = time() - s_time
            print("{}:{} => {} {} [{}]".format(p_count, s_count, l_count, e_time, e_count))
            save_json(file_path, p)
            data[p['id']] = p
    return data

kaptio_config_file = os.path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)
headers = {'Authorization': 'Keypair key={} secret={}'.format(kaptio_config['api']['auth']['key'], 
                                                                kaptio_config['api']['auth']['secret']),
          "Content-Type":"application/json"}

debug = True
baseurl = kaptio_config['api']['baseurl']

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

packageid = 'a754F0000000A30QAE'

if 'tax_profiles' in data:
    tax_profiles = data['tax_profiles']
else:
    # set up walker elements
    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA",
        "Foreign":"a8H4F0000003uJbUAI",
        "Domestic":"a8H4F0000003tnfUAA"
    }
    data['tax_profiles'] = tax_profiles

if 'occupancy' in data:
    occupancy = data['occupancy']
else:
    occupancy = {
        "single":"1=1,0",
        "double":"1=2,0",
        "double_child":"1=1,1",
        "triple":"1=3,0",
        "triple_1child":"1=2,1",
        "triple_2child":"1=1,2",
        "quad":"1=4,0",
        "quad_1child":"1=3,1",
        "quad_1child":"1=2,2",
        "quad_1child":"1=1,3"
    }
    data['occupancy'] = occupancy

if 'search_values' in data:
    search_values = data['search_values']
else:
    search_values = {
        "tax_profile_id":"a8H4F0000003tsfUAA",        # Required    #Zero
        "channel_id":'a6H4F0000000DkMUAU',            # Required    # travel agent
        "currency":"CAD",                             # Required
        "occupancy":"1=1,0",                          # Required
        "service_level_ids":"a7r4F0000000AloQAE", #,a7r4F0000004fd2QAA,a7r4F0000004nVbQAI
        "date_from":"2020-03-01",
        "date_to":"2020-10-31",
        "mode":"all",
        "filter":"id=={}".format(packageid)
    }
    data['search_values'] = search_values

if not 'season' in data:
    data['season'] = {}
    data['season']['start'] = '2020-04-01'
    data['season']['end'] = '2020-10-31'

season_start = data['season']['start']
season_end = data['season']['end']

if 'packages' in data:
    kt_packages = data['packages']
else:
    kt_packages = get_packages(headers, savepath, baseurl)
    print("\tFetched packages".format(len(kt_packages)))
    data['packages'] = kt_packages

for p in kt_packages:
    if not 'dates' in p:
        print("Fetching dates...{} => {}".format(p['id'], p['name']))
        p['dates'] = get_packagedepartures(headers, savepath, baseurl, p['id'], season_start, season_end)    
    
    # deal wit hteh custom fields
    for key, value in p['custom_fields'].items():
        if not key in p:
            p[key] = value

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)

if 'pricelist' in data:
    kt_pricelist = data['pricelist']
else:
    # do a short load....
    tax_profiles = {
        "Zero Rated":"a8H4F0000003tsfUAA"
    }
    occupancy = {
        "single":"1=1,0"
    }
    # run the pricelist load...
    kt_pricelist = get_extract(headers, savepath, baseurl, kt_packages, tax_profiles, occupancy, debug)
    data['pricelist'] = kt_pricelist

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)

# augment the kt_rpicelist with the pricelist info
for p in kt_packages:
    if not 'pricelist' in p:
        if 'id' in kt_pricelist:
            p['pricelist'] = kt_pricelist[p['id']]

file_path = os.path.join(savepath, "data", "kt_pricelist.json")
save_json(file_path, kt_pricelist)

file_path = os.path.join(savepath, "data", "kt_packages_aug.json")
save_json(file_path, kt_packages)

"""
# short list for proof
tax_profiles = {
    "Zero Rated":"a8H4F0000003tsfUAA"
}
occupancy = {
    "single":"1=1,0"
}

s_dates = list(sorted(kt_dates))[:1]
print("Calls per package: {}".format(len(tax_profiles) * len(occupancy) * len(s_dates)))

kt_augpackages = get_extract(headers, savepath, baseurl, kt_packages, s_dates, tax_profiles, occupancy)
for key, value in kt_augpackages.items():
    print(key)
"""

data['packages'] = kt_packages
save_pickle_data(data, pickle_file)