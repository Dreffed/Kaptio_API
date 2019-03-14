from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime

def extract_rows(node, fields):
    """ 
        node - a dictionary of data
        fields an array of dicts
            {
                "source":<>,
                "output":<>
                "translate": {<term>:<translate>, ...}
            }
    """
    row = {}
    if not isinstance(node, dict):
        return row
    #print("====\n{}".format(fields))
    for key, value in node.items():
        celldata = value
        if key in fields:
            if 'output' in fields[key]:
                outname = fields[key]['output']
            else:
                outname = key

            if 'fields' in  fields[key]:
                if '_type' in fields[key]:
                    if fields[key]["_type"] == 'list':
                        # this is a list object.
                        celldata = []
                        for item in value:
                            celldata.append(extract_rows(item, fields[key]['fields']))
                    elif fields[key]["_type"] == 'records':
                        celldata = []
                        for item in value[fields[key]["_type"]]:
                            celldata.append(extract_rows(item, fields[key]['fields']))
                    elif fields[key]["_type"] == 'dict':
                        celldata = extract_rows(value, fields[key]['fields'])
            row[outname] = celldata

    return row  

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

for key, value in data.items():
    print(key)

tax_profiles = {
    "Zero Rated":"a8H4F0000003tsfUAA",
    "Foreign":"a8H4F0000003uJbUAI",
    "Domestic":"a8H4F0000003tnfUAA"
}
occupancy = {
    "single":"1=1,0",
    "double":"1=2,0",
    "triple":"1=3,0",
    "quad":"1=4,0"    
} 

#"": {"output": ""},
#
fields = {
    "KaptioTravel__Category__c": {"output": "package_category"},
    "KaptioTravel__PackageStartLocation__c": {"output": "start_location_id"},
    "KaptioTravel__PackageEndLocation__c": {"output": "end_location_id"},
    "Name": {"output": "package_name"},
    "Direction__c": {"output": "package_direction"},
    "KaptioTravel__Length__c": {"output": "package_duration"},
    "ProductCode__c": {"output": "package_code"},
    "Id": {"output": "package_id"},
    "KaptioTravel__Content_Assignments__r": {
        "_type": "records",
        'output': "content",
        "fields": {
            "KaptioTravel__Body__c" : {"output":"content_description"},
            "LastModifiedDate" : {"output":"content_last_modifed"},
            "KaptioTravel__FeaturedImage__c" : {"output":"content_imageid"},
            "KaptioTravel__Sort__c" : {"output":"content_sort"},
            "KaptioTravel__Language__c" : {"output":"content_languageid"},
            "Id" : {"output":"content_id"},
            "Name" : {"output":"content_name"}
        },
    },
    "KaptioTravel__PackageDays__r": {
        "_type": "records",
        'output': "packagedays",
        "fields": {
            "KaptioTravel__Day__c" : {"output":"packageday_index"},
            #"KaptioTravel__Package__c" : {"output":""},
            "Id" : {"output":"packageday_id"},
            "Name" : {"output":"packageday_name"},
            "KaptioTravel__PackageInformations__r":{
                "_type": "records",
                'output': "packageinformation",
                "fields": {
                    "KaptioTravel__Key__c": {"outpu": "packageinfo_category"},
                    "KaptioTravel__PackageDay__c": {"output": "pacakgeinfo_packageday"},
                    "KaptioTravel__Language__c": {"output": "packageinfo_language"},
                    "Id": {"output": "packageinfo_id"},
                    "KaptioTravel__Value__c": {"output": "packageinfo_text"},
                    "Name": {"output": "packageinfo_name"}
                }
            },
            "KaptioTravel__PackageDayLocations__r":{
                "_type": "records",
                'output': "packagedaylocations",
                "fields": {
                    "type": {"output": "packgedaylocation_type"},
                    "KaptioTravel__PackageDay__c": {"output": "packageday_id"},
                    "KaptioTravel__Sort__c": {"output": "packagedaylocation_sort"},
                    "KaptioTravel__Location__c": {"output": "location_id"},
                    "Id": {"output": ""},
                    "Name": {"output": ""},
                    "KaptioTravel__Location__r":{
                        "_type": "dict",
                        'output': "packagedaylocations",
                        "fields": {
                            "KaptioTravel__RecordTypeDeveloperName__c": {"output": "location_type"},
                            "Id": {"output": "location_id"},
                            "KaptioTravel__FullLocationName__c": {"output": "location_path"},
                            "Name": {"output": "location_name"}
                        }
                    }
                }
            }
        }
    }
}

data['tax_profiles'] = tax_profiles
data['occupancy'] = occupancy

save_pickle_data(data, pickle_file)

kt_packages = []
if 'packages' in data:
    kt_packages = data['packages']

print("Loaded {} packages".format(len(kt_packages)))

for p in kt_packages:
    if 'pricelist' in p:
        if 'errors' in p['pricelist']:
            print("{} has pricelist".format(p['id']))
            print("\tERROR Found: {}".format(p['pricelist']['errors']))

if 'content' in data:
    kt_content = data['content']

rows = []
for key, value in kt_content.items():
    if isinstance(value, list):
        for item in value:
            row = extract_rows(item, fields) 
            rows.append(row)

print(len(rows))
print(json.dumps(rows, indent=4))





