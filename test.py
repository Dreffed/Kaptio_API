import requests
import json

# Load credentials from json file
with open("kaptio_settings.json", "r") as file:  
    settings = json.load(file)

url = r"https://api.kaptio.com/v1.0/promotions"
url = r'https://api.kaptio.com/v2.0/allotment_days?limit=200'
headers = {'Authorization': 'Keypair key={} secret={}'.format(settings['auth']['key'], settings['auth']['secret'])}

r = requests.get(url, headers=headers)

print(r)