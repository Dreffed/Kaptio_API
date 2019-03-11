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
    {"name":"Items", "version":"v1.0", "suburl":"items", "method":"GET"},
    {"name":"Items", "version":"v2.0", "suburl":"items", "method":"GET"},
    {"name":"Items Item", "version":"v1.0", "suburl":"items", "query":["{itemId}"], "method":"GET"},
    {"name":"Items Inventory", "version":"v1.0", "suburl":"items", "query":["{itemId}", "inventory"], "body":["item_option_id", "date_from", "date_to", "channel_id", "package_id", "account_id", "flex_days"], "method":"POST"},
    {"name":"Items Inventory Many", "version":"v1.0", "suburl":"inventory", "query":["{itemId}", "inventory", "many"], "body":["item_option_id", "dates: [{date_from, date_to}]", "channel_id", "package_id", "account_id"], "method":"POST"},
    {"name":"Items Inventory Bulk", "version":"v1.0", "suburl":"items/inventory_bulk", "body":["index", "item_id", "item_option_id", "date_from", "date_to", "channel_id", "package_id", "account_id", "flex_days"], "method":"POST"},
    {"name":"Items Prices", "version":"v1.0", "suburl":"items/prices_bulk", "body": ["channel_id", "currecny", "customer_price", ""],"method":"POST"},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""},
    {"name":"", "version":"", "suburl":"", "method":""}
]

headers = {'Authorization': 'Keypair key={} secret={}'.format(settings['api']['auth']['key'], settings['api']['auth']['secret'])}

values = {
    "limit":200,
    "{channelId}":"a6H4F0000000DkbUAE",
    "{itemId}": 'a6k4F0000002BuWQAU',
    "{pacakgeId}":'a754F0000000A30QAE'
}

for url in urls:
    if len(url['suburl']) > 0:
        paramstr = ""
        querystr = ""
        bodystr = ""

        if  "params" in url:
            paramitems = []
            for p in url['params']:
                if p in values:
                    paramitems.append("{}={}".format(p, values[p]))
            if len(paramitems) > 0:
                paramstr = "?{}".format("+".join(paramitems))

        if "query" in url:
            queryitems = []
            for q in url['query']:
                if q in values:
                    queryitems.append("{}".format(values[q]))
                elif not q.startswith("{"):
                    queryitems.append(q)
            if len(queryitems) > 0:
                querystr = "/{}".format("/".join(queryitems))

        if "body" in url:
            continue

        thisurl = 'http://{}/{}/{}{}{}'.format(baseurl, url['version'], url['suburl'], paramstr, querystr)
        print("{}:{}\n\t{}". format(url["name"], url['method'], thisurl))

        if url['method'] == "GET":
            r = requests.get(thisurl, headers=headers)
        elif url['method'] == "POST":
            r = requests.post(thisurl, headers=headers)
        
        print(r)
