from time import time
import json
import os
import requests
from datetime import datetime
from kaptiorestpython.helper.http_lib import HttpLib
from kaptiorestpython.helper.exceptions import APIException
from utils import save_json
from simple_salesforce import Salesforce
import requests

class KaptioOGraph:
    # franken code for client calls...
    baseurl = None
    username = None
    password = None
    security_token = None
    sandbox = None
    clientid = None
    clientsecret = None
    access_token = None
    instance_url = None

    def __init__(self, baseurl, auth_key, auth_secret):
        
        pass
    
    def connect_sf(self):
        sf = Salesforce(username=self.username, password=self.password, security_token=self.token, sandbox=True)
        return sf

    def get_token(self):
        params = {
            "grant_type": "password",
            "client_id": self.clientid,
            "client_secret": self.clientsecret,
            "username": self.username,
            "password": "{}{}".format(self.password, self.token)
        }
        r = requests.post("{}/services/oauth2/token".format(self.url), params=params)
        self.access_token = r.json().get("access_token")
        self.instance_url = r.json().get("instance_url")
        print("Access Token:", access_token)
        print("Instance URL", instance_url)        

    