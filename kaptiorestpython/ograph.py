from time import time
import json
import os
import requests
from urllib.parse import quote
from datetime import datetime
from kaptiorestpython.helper.http_lib import HttpLib, _rate_limited
from kaptiorestpython.helper.exceptions import APIException
from utils import save_json
from simple_salesforce import Salesforce
import requests
import logging

class KaptioOGraph:
    # franken code for client calls...
    baseurl = None
    sfurl = None
    username = None
    password = None
    security_token = None
    sandbox = None
    clientid = None
    clientsecret = None
    access_token = None
    instance_url = None
    num_calls_per_second = 5

    def __init__(self, baseurl, sfurl, username, password, security_token, sandbox, clientid, clientsecret, use_sandbox=True):
        self.logger = logging.getLogger(__name__)
        assert(baseurl is not None)
        assert(sfurl is not None)
        assert(username is not None)
        assert(password is not None)
        assert(security_token is not None)
        assert(sandbox is not None)
        assert(clientid is not None)
        assert(clientsecret is not None)


        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.baseurl = baseurl
        self.sfurl = sfurl
        self.username = username
        self.password = password
        self.security_token = security_token
        self.sandbox = sandbox
        self.clientid = clientid
        self.clientsecret = clientsecret
        self.use_sandbox = use_sandbox

    def connect_sf(self):
        sf = Salesforce(username=self.username, password=self.password, security_token=self.security_token, sandbox=self.use_sandbox)
        self.logger.info("connected to SFDC {}".format(self.sfurl))
        return sf

    def get_token(self):
        params = {
            "grant_type": "password",
            "client_id": self.clientid,
            "client_secret": self.clientsecret,
            "username": self.username,
            "password": "{}{}".format(self.password, self.security_token)
        }
        r = requests.post("{}/services/oauth2/token".format(self.sfurl), params=params)
        self.access_token = r.json().get("access_token")
        self.instance_url = r.json().get("instance_url")
        self.logger.info("Access Token: {}".format(self.access_token))
        self.logger.info("Instance URL: {}".format(self.instance_url))      

    @_rate_limited(num_calls_per_second)
    def process_query(self, query):
        if self.access_token is None:
            self.get_token()
        
        if self.access_token is None:
            raise Exception("Unable to connect to SFDC {} => {}".format(self.sfurl, self.baseurl))
    
        content_url = r"{}/services/data/v44.0/query/?q={}".format(self.sfurl, quote(query))
        content_hdr = {
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
            "Authorization": "Bearer {}".format(self.access_token),
            "cache-control": "no-cache"
        }

        json_data = {}
        r = requests.get(content_url, headers=content_hdr)

        if r.status_code == 200:
            json_data = r.json()
        else:
            json_data['Error'] = r
            self.logger.error("Failed: {} => {}".format(r, r.text)) 
        return json_data

    @_rate_limited(num_calls_per_second)
    def get_content(self, packageid):
        if self.access_token is None:
            self.get_token()
        
        if self.access_token is None:
            raise Exception("Unable to connect to SFDC {} => {}".format(self.sfurl, self.baseurl))

        content_url = r"{}/services/apexrest/kaptio/packagecontent/{}".format(self.baseurl, packageid)
        content_hdr = {
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
            "Authorization": "Bearer {}".format(self.access_token),
            "cache-control": "no-cache"
        }

        json_data = {}
        r = requests.get(content_url, headers=content_hdr)
        if r.status_code == 200:
            json_data = json.loads(r.text)
        else:
            json_data['Error'] = r
            self.logger.info("Failed: {} => {}".format(r, r.text)) 
        return json_data


    