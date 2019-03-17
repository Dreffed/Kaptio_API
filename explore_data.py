from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json, extract_rows
import json
import pickle
import os
import path
from time import time
from datetime import datetime

update_lookups = False

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

pickle_files = scanfiles('.', r".*\.pickle")
print("Pickles:")
for f in pickle_files:
    print("\t{} => {} [{}]".format(f['file'], f['folder'], json.dumps(f, indent=2)))

pickle_file = "kaptio_allsell.pickle"

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
contentconfigpath = "content_fields.json"
fields = load_json(contentconfigpath)

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

""""
<RockyMountaineer>
    <products bookingtype="{tax_profile}" date="{generateddate}" >
        <product code="{}" shortcode="{productcode}" name="{productname}"
            <packageType>{}</packageType>
            <duration>{}</duration>
            <cityStays>{}</cityStays>
            <departureCity>{}</departureCity>
            <destinationCity>{}</destinationCity>
            <packageImage/>
            <routeMap>{}</routeMap>
            <promotion>false</promotion>
            <webItinerary>
                <day num="{}">
                    <StartCity>{}}</StartCity>
                    <EndCity>{}</EndCity>
                    <Description>{}</Description>
                    <Breakfast>{}</Breakfast>
                    <Lunch>{}</Lunch>
                    <Dinner>{}</Dinner>
                </day>
                ...
            </webItinerary>
            <highlights>
                <highlight>{}</highlight>
                ...
            </highlights>
            <seatClasses>
                <seatClass type="{}">
                <fareBases>
                    <fareBasis type="{}">
                    <prices from="{}" to="{}" alt="0.00">
                        <adult>
                            <price>{}</price>
                            <tax>{}</tax>
                        </adult>
                        <child>
                            <price>{}</price>
                            <tax>{}</tax>
                        </child>
                        <infant>
                            <price>{}</price>
                            <tax>{}}</tax>
                        </infant>
                    </prices>
                    ...
                    <serviceItinerary>
                        <service day="{}" itemSequence="{}" serviceSequence="{}">
                            <name>{}</name>
                            <type>{}</type>
                            <region>{}</region>
                            <text language="English"><![CDATA[{}]]></text>
                            <room>{}</room>
                        </service>
                        ...
                    </serviceItinerary>
                </fareBasis>
                ...
            </seatClasses>                    
        </product>
        ...
    </products>
    ...
</RockyMountaineer>

"""



