# load the dependancies
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, scan_packagefiles
import json
import pickle
import os
import path
from time import time
from datetime import datetime
import socket
import csv

hostname = socket.gethostname()
homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

datestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# get the processed files data:
kt_processed = scan_packagefiles(savepath)
print("Found {} fresh price packages".format(len(kt_processed)))

kt_packages = data.get('packages', {})
print("Found {} cached packages".format(len(kt_packages)))

kt_skipped = []

package_count = 0
change_count = 0
for p in kt_packages:
    packageid = p.get('id')
    package_count += 1
    if packageid:
        p_data = kt_processed.get(packageid, {})
        if not p_data:
            kt_skipped.append(packageid)
            continue

        p_data['updated'] = datestamp
        p_data['hostname'] = hostname
        if p_data:
            change_count +=1
            p = p_data

print("Packages: {}\n\tskipped: {}\n\tChanged: {}".format(package_count, len(kt_skipped), change_count))
if change_count > 0:
    data['packages'] = kt_packages
    save_pickle_data(data, pickle_file)
"""
change_count = 0
for packageid in kt_skipped:
    p_data = kt_processed.get(packageid, {})
    kt_packages.append(p_data)
    change_count += 1

print("Packages: {}\n\tChanged: {}".format(len(kt_packages), change_count))

if change_count > 0:
    data['packages'] = kt_packages
    save_pickle_data(data, pickle_file)
"""
kt_error = []
kt_1040 = []
kt_1020 = []

package_count = 0
error_count = 0

for p in kt_packages:
    packageid = p.get('packageid')
    datestamp = p.get('updated')
    hostname = p.get('hostname')
    if not packageid:
        continue
    print("{} => {} {}".format(packageid, datestamp, hostname))

    # scan and pull the errors...
    for d_key, d_value in p('pricelist', {}).items():
        for t_key, t_value in d_value.items():
            if t_key == "errors":
                continue
            for o_key, o_value in t_value.items():
                for p_item in o_value:
                    if packageid == 'a754F0000000A2qQAE':
                        print(p_item)

                    for e_item in p_item.get('errors', []):
                        e_code = e_item.get('error', {}).get('code') 
                        log_item = {
                            'packageid': packageid,
                            'date': d_key,
                            'tax_profile': t_key,
                            'occupancy': o_key,
                            'service_level_id': p_item.get('service_level_id'),
                            'error': e_item
                        }
                        print("===\n{}".format(json.dumps(log_item, indent=4)))

                        if e_code == 1040:
                            kt_1040.append(log_item)
                        elif e_code == 1020:
                            kt_1020.append(log_item)
                        else:
                            kt_error.append(log_item)

print("Packages: {}\n\tMisc:{}\n\t1020:{}\n\t1040:{}".format(len(data.get('packages', [])), len(kt_error), len(kt_1020), len(kt_1040)))

if len(kt_1020) > 0:
    with open('product_launch_1020.csv', 'w', newline='') as csvfile:
        fieldnames = ['packageid', 'date', 'tax_profile', 'occupancy', 'service_level_id', 'error']
        writer = csv.DictWriter(kt_1020, fieldnames=fieldnames)

if len(kt_1040) > 0:
    with open('product_launch_1040.csv', 'w', newline='') as csvfile:
        fieldnames = ['packageid', 'date', 'tax_profile', 'occupancy', 'service_level_id', 'error']
        writer = csv.DictWriter(kt_1040, fieldnames=fieldnames)

if len(kt_error) > 0:
    with open('product_launch_errors.csv', 'w', newline='') as csvfile:
        fieldnames = ['packageid', 'date', 'tax_profile', 'occupancy', 'service_level_id', 'error']
        writer = csv.DictWriter(kt_error, fieldnames=fieldnames)