import requests
import json

# Load credentials from json file
with open("kaptio_settings.json", "r") as f:  
    settings = json.load(f)

baseurl = settings['api']['baseurl']

url = r"https://api.kaptio.com/v1.0/promotions"
url = r'http://{}/v2.0/allotment_days?limit=200'.format(baseurl)

urls = [
    {"name":"Alloment Days", "version":"v2.0", "suburl":"allotment_days", "params":["limit", "next"], "method":"GET"},
    {"name":"Channels", "version":"v1.0", "suburl":"channels", "method":"GET"},
    {"name":"Channels", "version":"v2.0", "suburl":"channels", "params":["limit", "next"], "method":"GET"},
    {"name":"Channel Item", "version":"v1.0", "suburl":"channels", "query":["{channelId}"], "method":"GET"},
    {"name":"Config", "version":"v1.0", "suburl":"client/config", "method":"GET"},
    {"name":"Items", "version":"v1.0", "suburl":"items", "params":[""], "method":"GET"},
    {"name":"Items", "version":"v2.0", "suburl":"items", "params":[""], "method":"GET"},
    {"name":"Items Item", "version":"v1.0", "suburl":"items", "query":["{itemId}"], "method":"GET"},
    {"name":"Items Inventory", "version":"v1.0", "suburl":"items", "query":["{itemId}", "inventory"], "method":"POST"},
    {"name":"Items Inventory Many", "version":"", "suburl":"", "params":[""], "method":""},
    {"name":"", "version":"", "suburl":"", "params":[""], "method":""},
    {"name":"", "version":"", "suburl":"", "params":[""], "method":""},
    {"name":"", "version":"", "suburl":"", "params":[""], "method":""},
    {"name":"", "version":"", "suburl":"", "params":[""], "method":""}
]

headers = {'Authorization': 'Keypair key={} secret={}'.format(settings['auth']['key'], settings['auth']['secret'])}

paramvalues = {
    "limit":200
}

queryvalues = {
    "{channelId}":"a6H4F0000000DkbUAE"
}

for url in urls:
    if len(url['suburl']) > 0:
        if  "params" in url > 0:
            paramitems = []
            for p in url['params']:
                if p in paramvalues:
                    paramitems.append("{}={}".format(p, paramvalues[p]))
            paramstr = "?{}".format("+".join(params))

        if "query" in url:
            queryitems = []
            for q in url['query']:
                if q in queryvalues:
                    queryitems.append("{}".format(queryvalues[q]))
            querystr = "/{}".format("/".join(queryitems))

        thisurl = 'http://{}/{}/{}{}'.format(baseurl, url['version'], url['suburl'], paramstr)

        print(thisurl)
        if url['method'] == "GET":
            r = requests.get(thisurl, headers=headers)
        elif url['method'] == "POST":
            r = requests.post(thisurl, headers=headers)
        
        print(r)
