from time import time, sleep
import json
import os
import requests
from datetime import datetime
from kaptiorestpython.helper.http_lib import HttpLib, _rate_limited
from kaptiorestpython.helper.exceptions import APIException
from kaptiorestpython.utils_kaptio import has_empty_warning, load_kaptioconfig, display_fields
from utils import save_json, scan_packagefiles
import multiprocessing
from multiprocessing.queues import Empty
import logging.config

class KaptioSearch:
    '''This is a search class that will generate the search terms, and 
    used to pass in the parameteres to the  underlying functions'''
    search_values = dict()
    req_terms = [
        'tax_profile_id',
        'channel_id',
        'currency,'
        'occupancy'
    ]
    option_terms = [
        'service_level_ids',
        'date_from',
        'date_to',
        'mode'
    ]
    filter_terms = [
        'id',
        'length',
        'category',
        'start_location',
        'end_location',
        'record_type_name'
    ]

    def __init__(self, search_values):
        '''
        Pass in a dict of terms, Kaptio require the following:
            tax_profile_id
            channel_id
            currency
            occupancy

        They also accept the following:
            service_level_ids
            date_from
            date_to
            mode

        And they have a filter which is a weird list format
            filter
        '''
        self.search_values = search_values

    def updatecreate_term(self, term, value):
        ''' Adds a terms to the search_values dict
        '''
        self.search_values[term] = value

    def add_filter(self, term, operator, value):
        ''' Uses the filter terms to add in a value to the filter part
        '''
        pass

    def build_query(self):
        ''' This will return an empty string is no terms,
        other wise it'll build out the string
        '''
        search_list = []
        for key, value in self.search_values.items():
            if len(value) > 0:
                search_list.append("{}={}".format(key, value))

        if len(search_list) > 0:
            return "?{}".format("&".join(search_list))   
        return ""

class KaptioClient:
    # franken code for client calls...
    
    baseurl = None
    auth_key = None
    auth_secret = None
    headers = None
    num_calls_per_second = 5

    def __init__(self, baseurl, auth_key, auth_secret):
        assert(baseurl is not None)
        assert(auth_key is not None)
        assert(auth_secret is not None)

        self.logger = logging.getLogger(__name__)

        self.baseurl = baseurl
        self.auth_key = auth_key
        self.auth_secret = auth_secret
        self.headers = {'Authorization': 'Keypair key={} secret={}'.format(auth_key, auth_secret),
          "Content-Type":"application/json"}

    @_rate_limited(num_calls_per_second)
    def api_data(self, url_data, paramstr ="", querystr = "", body = None):
        thisurl = 'http://{}/{}/{}{}{}'.format(self.baseurl, url_data['version'], url_data['suburl'], paramstr, querystr)
        self.logger.debug("{}:{}\n\t{}". format(url_data["name"], url_data['method'], thisurl))
        try:
            if url_data['method'] == "GET":
                r = requests.get(thisurl, headers=self.headers)
            elif url_data['method'] == "POST":
                r = requests.post(thisurl, headers=self.headers, json=body)
                
            return r
        except:
            return None

    @_rate_limited(num_calls_per_second)
    def api_list(self, url_data, paramstr, querystr, body = None):
        thisurl = 'http://{}/{}/{}{}{}'.format(self.baseurl, url_data['version'], url_data['suburl'], paramstr, querystr)
        self.logger.debug("{}:{}\n\t{}". format(url_data["name"], url_data['method'], thisurl))
        data = []
        while True:
            if url_data['method'] == "GET":
                r = requests.get(thisurl, headers=self.headers)
            elif url_data['method'] == "POST":
                r = requests.post(thisurl, headers=self.headers, json=body)
            
            if r.status_code != 200:
                self.logger.info('ERROR: {} => {}'.format(r, r.text))
                break
            
            json_data = json.loads(r.text)
            self.logger.debug('====')
            #display_fields(json_data)
            data.extend(json_data['records'])
            
            if json_data['next']:
                thisurl = json_data['next']
            else:
                break

        return data

    def save_response(self, savepath, base_name, resp, field_name):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        json_data = json.loads(resp.text)
        suffix_name = field_name
        if field_name in json_data:
            suffix_name = json_data[field_name]
        
        json_path = os.path.join(savepath, "data", "{}_{}_{}.json".format(base_name,suffix_name, timestamp))
        save_json(json_path, json_data)
        return json_data

    def get_responsedata(self, resp):
        json_data = json.loads(resp.text)
        return json_data
        
    def get_channels(self, savepath):
        url_data = {}
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        url_data['name'] = "Channels"
        url_data['version'] = 'v2.0'
        url_data['suburl'] = 'channels'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        data = self.api_list(url_data, paramstr, querystr)
        file_path = os.path.join(savepath, "data", "kt_channels_{}.json".format(timestamp))
        save_json(file_path, data)
        return data

    def get_client(self, savepath):
        url_data = {}
        url_data['name'] = "Client"
        url_data['version'] = 'v1.0'
        url_data['suburl'] = 'client/config'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        r = self.api_data( url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, 'kt', r, 'client')
            display_fields(data)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text))    
        return data

    def get_package(self, savepath, packageid):
        url_data = {}
        url_data['name'] = "Package"
        url_data['version'] = 'v1.0'
        url_data['suburl'] = 'packages'
        url_data['method'] = 'GET'
        paramstr = '/{}'.format(packageid)
        querystr = ''

        r = self.api_data(url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, url_data['name'], r, 'id')
            if self.logger.level == logging.DEBUG:
                display_fields(data)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text))
        return data

    def get_packages(self, savepath):
        # build out all the packages
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        url_data = {}
        url_data['name'] = "All packages"
        url_data['version'] = 'v2.0'
        url_data['suburl'] = 'packages'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        data = self.api_list( url_data, paramstr, querystr)
        if self.logger:
            file_path = os.path.join(savepath, "data", "kt_packages_{}.json".format(timestamp))
            save_json(file_path, data)

        return data
        
    def get_search(self, savepath, packageid, search_values):
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

        r = self.api_data( url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, url_data['name'], r, save_str)
            display_fields(data)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text))    
        return data

    def get_packageadv(self, savepath, packageid, search_values):
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

        r = self.api_data( url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, url_data['name'], r, packageid)
            #display_fields(data)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text))    
        return data

    def get_packagedepartures(self, savepath, packageid, datefrom, dateto):
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

        r = self.api_data( url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, url_data['name'], r, packageid)
            return data
        else:
            self.logger.info("Failed: {} => {}\n\t{}\n\t{}\n\t{}".format(r, r.text, json.dumps(url_data, indent=2), querystr, paramstr))    
        return {}

    def get_item(self, savepath, itemid):
        url_data = {}
        url_data['name'] = "Item"
        url_data['version'] = 'v1.0'
        url_data['suburl'] = 'items'
        url_data['method'] = 'GET'
        paramstr = '/{}'.format(itemid)
        querystr = ''

        r = self.api_data( url_data, paramstr, querystr)
        if r.status_code == 200:
            data = self.save_response(savepath, url_data['name'], r, 'id')
            display_fields(data)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text))

        return data

    def get_items(self, savepath):
        # get all the items
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        url_data = {}
        url_data['name'] = "All items"
        url_data['version'] = 'v2.0'
        url_data['suburl'] = 'items'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        data = self.api_list( url_data, paramstr, querystr)
        file_path = os.path.join(data, "data", "kt_items_{}.json".format(timestamp))
        try:
            save_json(file_path, data)
        except: 
            pass
        return data

    def get_servicelevels(self, savepath):
        # get all the servicelevels
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        url_data = {}
        url_data['name'] = "All Servicelevels"
        url_data['version'] = 'v2.0'
        url_data['suburl'] = 'service_levels'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        data = self.api_list( url_data, paramstr, querystr)
        self.logger.debug("found: {}".format(len(data)))
        
        if self.logger:
            file_path = os.path.join(savepath, "data", "kt_servicelevels_{}.json".format(timestamp))
            try:
                save_json(file_path, data)
            except: 
                pass
        return data

    def get_languages(self, savepath):
        # get all the languages
        url_data = {}
        url_data['name'] = "All Languages"
        url_data['version'] = 'v2.0'
        url_data['suburl'] = 'languages'
        url_data['method'] = 'GET'
        paramstr = ''
        querystr = ''

        data = self.api_list( url_data, paramstr, querystr)
        file_path = os.path.join(savepath, "data", "kt_lauguages.json")
        try:
            save_json(file_path, data)
        except: 
            pass

    def get_packageprice_query(self, savepath, packageid, querystr):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        url_data = {}
        url_data['name'] = "Package Search"
        url_data['version'] = 'v1.0'
        url_data['suburl'] = 'packages/search'
        url_data['method'] = 'GET'
        paramstr = ''

        r = self.api_data(url_data, paramstr, querystr)
        if r.status_code == 200:
            rd = self.get_responsedata(r)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text)) 
            return [{"resp": r}]
            
        if self.logger.level == logging.DEBUG:
            try:
                filepath = os.path.join(savepath, "data", "price_{}_{}.json".format(packageid, timestamp))
                json_msg = {
                    "packageid":packageid,
                    "query": querystr,
                    "resp": rd
                }

                with open(filepath, 'a') as f:
                    json.dump(json_msg, f, indent=4)
            except:
                pass
        return rd

    def get_packageprice(self, savepath, packageid, date_from, date_to, 
                        taxprofileid = 'a8H4F0000003tsfUAA', channelid = 'a6H4F0000000DkMUAU', 
                        occupancy = '1=1,0', services = 'a7r4F0000000AloQAE', currency="CAD"):
        data = []
        errors = []
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    
        url_data = {}
        url_data['name'] = "Package Search"
        url_data['version'] = 'v1.0'
        url_data['suburl'] = 'packages/search'
        url_data['method'] = 'GET'
        paramstr = ''
        
        search_values = {
            "tax_profile_id":taxprofileid,  # Required    #Zero
            "channel_id":channelid,         # Required    # travel agent
            "currency":currency,               # Required
            "occupancy":occupancy,          # Required
            "service_level_ids":services,   
            "date_from":date_from,
            "date_to":date_to,
            "mode":"",
            "filter":"id=={}".format(packageid)
        }

        search_list = []
        for key, value in search_values.items():
            if len(value) > 0:
                search_list.append("{}={}".format(key, value))

        if len(search_list) > 0:
            querystr = "?{}".format("&".join(search_list))   

        r = self.api_data(url_data, paramstr, querystr)
        if r.status_code == 200:
            rd = self.get_responsedata(r)
        else:
            self.logger.info("Failed: {} => {}".format(r, r.text)) 
            return data

        for p in rd['results']:
            for d in p['prices_by_service_level']:
                if len(d['errors']) > 0:
                    errors.append(d)
                else:
                    data.append(d)
            
        if self.logger.level == logging.DEBUG:
            try:
                filepath = os.path.join(savepath, "data", "price_{}_{}.json".format(packageid, timestamp))
                json_msg = {
                    "packageid":packageid,
                    "query": querystr,
                    "resp": rd
                }  

                with open(filepath, 'a') as f:
                    json.dump(json_msg, f, indent=4)
            except:
                pass

        return {'data':data, 'errors':err}

    def walk_package(self, savepath, packageid, dates, tax_profiles, occupancy, services, channelid="a6H4F0000000DkbUAE", currency="CAD"):
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
                    pricelist = self.get_packageprice(savepath, packageid, date_from=d, date_to=d, 
                                                taxprofileid=t_value, occupancy=o_value,
                                                services=services_str, channelid=channelid, currency=currency)
                    data[d][t_key][o_key] = []
                    try:
                        if len(pricelist['data']) > 0:
                            data[d][t_key][o_key].extend(pricelist['data'])
                        if len(pricelist['errors']) > 0:
                            data[d][t_key][o_key].extend(pricelist['errors'])
                            if not 'errors' in data:
                                data['errors'] = 0
                            data['errors'] += len(pricelist['errors'])
                        c_count += 1
                    except:
                        if not 'errors' in data:
                            data['errors'] = 0
                        data['errors'] += 1
                        data[d][t_key][o_key] = [{"errors" : [{"room_index": 0, "error": {"code": 500, "message": "Internal Server Error 500", "details": ""}}]}]
        self.logger.info("\t{} => {}".format(packageid, c_count))
        return data

    def get_extract(self, savepath, packages, tax_profiles, occupancy, channelid="a6H4F0000000DkbUAE", debug=False):
        p_count = 0
        s_count = 0
        l_count = 0
        e_count = 0
        s_time = time()
        self.logger.info("{}:{} => {} {} [{}]".format(p_count, s_count, l_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        kt_processed = scan_packagefiles(savepath)
        
        data = {} 
        for p in packages:
            p_count += 1
            packageid = p.get('id')

            if kt_processed.get(packageid):
                self.logger.info("Skipping {}: already downloaded".format(packageid))
                continue

            if p['record_type_name'] == 'Package' and p['active']:
                if p['name'].lower().startswith('test'):
                    continue
                s_count += 1
                file_path = os.path.join(savepath, "data", "kt_package_{}_{}.json".format(p['id'], timestamp))

                p['services'] = ""
                p['direction'] = ""
                p['product_code'] = ""
                p['services'] = [s.get('id') for s in p.get('service_levels', [])]
                
                p['direction'] = p.get('custom_fields',{}).get('direction')
                p['product_code'] = p.get('custom_fields',{}).get('product_code')

                if not 'pricelist' in p:
                    p['pricelist'] = self.walk_package(
                                savepath, packageid, dates=p['dates'], 
                                tax_profiles=tax_profiles, channelid=channelid,
                                occupancy=occupancy, 
                                services=p['service_levels'])
                else:
                    if 'errors' in p['pricelist']:
                        self.logger.error("Fixing errors: {} => {}".format(p['id'], p['name']))
                        p['pricelist'] = self.walk_package(savepath, p['id'], dates=p['dates'], tax_profiles=tax_profiles, occupancy=occupancy, services=p['service_levels'])
                    else:
                        self.logger.info("Skipping: {} => {}".format(p['id'], p['name']))

                e_time = time() - s_time
                self.logger.info("{}:{} => {} {} [{}]".format(p_count, s_count, l_count, e_time, e_count))
                try:
                    save_json(file_path, p)
                except: 
                    pass
                data[p['id']] = p
        return data

