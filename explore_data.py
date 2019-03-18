from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime

update_lookups = False

def extract_rows(node, fields):
    """ 
        node - a dictionary of data
        fields an array of dicts
            {
                "source":<>,
                "_output":<>
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
            if '_output' in fields[key]:
                outname = fields[key]['_output']
            else:
                outname = key

            if '_fields' in  fields[key]:
                if '_type' in fields[key]:
                    if fields[key]["_type"] == 'list':
                        # this is a list object.
                        celldata = []
                        for item in value:
                            celldata.append(extract_rows(item, fields[key]['_fields']))
                    elif fields[key]["_type"] == 'records':
                        celldata = []
                        for item in value[fields[key]["_type"]]:
                            celldata.append(extract_rows(item, fields[key]['_fields']))
                    elif fields[key]["_type"] == 'dict':
                        celldata = extract_rows(value, fields[key]['_fields'])
            row[outname] = celldata

    return row  

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

pickle_files = scanfiles('.', r".*\.pickle")
print("Pickles:")
for f in pickle_files:
    print("\t{} => {} [{}]".format(f['file'], f['folder'], json.dumps(f, indent=2)))

pickle_file = "kaptio_allsell.pandarus.pickle"

data = get_pickle_data(pickle_file)

for key, value in data.items():
    print(key)

if update_lookups:
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

    data['tax_profiles'] = tax_profiles
    data['occupancy'] = occupancy

    save_pickle_data(data, pickle_file)

#"": {"_output": ""},
#
fields = {
    "KaptioTravel__Category__c": {"_output": "package_category"},
    "KaptioTravel__PackageStartLocation__c": {"_output": "package_start_location_id"},
    "KaptioTravel__PackageEndLocation__c": {"_output": "package_end_location_id"},
    "Name": {"_output": "package_name"},
    "Direction__c": {"_output": "package_direction"},
    "KaptioTravel__Length__c": {"_output": "package_duration"},
    "ProductCode__c": {"_output": "package_code"},
    "Id": {"_output": "package_id"},
    "KaptioTravel__Content_Assignments__r": {
        "_type": "records",
        "_output": "content",
        "_fields": {
                "KaptioTravel__Content__r":{
                    "_type": "dict",
                    "_output": "content",
                    "_fields": {
                        "KaptioTravel__Body__c" : {"_output":"content_description"},
                        "LastModifiedDate" : {"_output":"content_last_modifed"},
                        "KaptioTravel__Sort__c" : {"_output":"content_sort"},
                        "KaptioTravel__Language__c" : {"_output":"content_languageid"},
                        "KaptioTravel__FeaturedImage__c" : {"_output":"content_imageid"},
                        "Id" : {"_output":"content_id"},
                        "Name" : {"_output":"content_name"}
                    }
                }
        },
    },
    "KaptioTravel__PackageDays__r": {
        "_type": "records",
        "_output": "packagedays",
        "_fields": {
            "KaptioTravel__Day__c" : {"_output":"packageday_index"},
            #"KaptioTravel__Package__c" : {"_output":""},
            "Id" : {"_output":"packageday_id"},
            "Name" : {"_output":"packageday_name"},
            "KaptioTravel__PackageInformations__r":{
                "_type": "records",
                "_output": "packageinformation",
                "_fields": {
                    "KaptioTravel__Key__c": {"_output": "packageinfo_category"},
                    "KaptioTravel__PackageDay__c": {"_output": "packageinfo_packageday"},
                    "KaptioTravel__Language__c": {"_output": "packageinfo_language"},
                    "Id": {"_output": "packageinfo_id"},
                    "KaptioTravel__Value__c": {"_output": "packageinfo_text"},
                    "Name": {"_output": "packageinfo_name"}
                }
            },
            "KaptioTravel__PackageDayLocations__r":{
                "_type": "records",
                "_output": "packagedaylocations",
                "_fields": {
                    "type": {"_output": "packgedaylocation_type"},
                    "KaptioTravel__PackageDay__c": {"_output": "packageday_id"},
                    "KaptioTravel__Sort__c": {"_output": "packagedaylocation_sort"},
                    "KaptioTravel__Location__c": {"_output": "location_id"},
                    "Id": {"_output": "packagedaylocation_id"},
                    "Name": {"_output": "pcakgedaylocation_name"},
                    "KaptioTravel__Location__r":{
                        "_type": "dict",
                        "_output": "packagedaylocations",
                        "_fields": {
                            "KaptioTravel__RecordTypeDeveloperName__c": {"_output": "location_type"},
                            "Id": {"_output": "location_id"},
                            "KaptioTravel__FullLocationName__c": {"_output": "location_path"},
                            "Name": {"_output": "location_name"}
                        }
                    }
                }
            }
        }
    }
}

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

file_path = os.path.join(savepath, "data", "kt_contents_{}.json".format(timestamp))
save_json(file_path, kt_content)

file_path = os.path.join(savepath, "data", "kt_content_rows_{}.json".format(timestamp))
save_json(file_path, rows)





