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
price_count = 0
error_count = 0

tax_profiles = data.get('tax_profiles', {})

#for p_value in kt_packages:
for p_key, p_value in kt_processed.items():
    packageid = p_value.get('id')
    datestamp = p_value.get('updated')
    hostname = p_value.get('hostname')
    package_count += 1
    if not packageid:
        continue

    service_levels = p_value.get('service_levels', {})
    sl_rev = {}
    for s_item in service_levels:
        sl_id = s_item.get('id')
        if sl_id:
            sl_rev[sl_id] = s_item

    # scan and pull the errors...
    for d_key, d_value in p_value.get('pricelist', {}).items():
        if d_key == "errors":
            continue        
        for t_key, t_value in d_value.items():
            if t_key == "errors":
                continue

            for o_key, o_value in t_value.items():
                for p_item in o_value:
                    price_count += 1

                    for e_item in p_item.get('errors', []):
                        error_count += 1
                        e_code = e_item.get('error', {}).get('code') 
                        log_item = {
                            'packageid': packageid,
                            'date': d_key,
                            'tax_profile_id': tax_profiles.get(t_key),
                            'tax_profile': t_key,
                            'occupancy': o_key,
                            'service_level_id': p_item.get('service_level_id'),
                            'service_level': sl_rev.get(p_item.get('service_level_id'),{}).get('name'),
                            'code': e_code,
                            'message': e_item.get('error', {}).get('message') 
                        }

                        if e_code == 1040:
                            kt_1040.append(log_item)
                        elif e_code == 1020:
                            kt_1020.append(log_item)
                        else:
                            kt_error.append(log_item)

print("Packages: {}".format(len(data.get('packages', []))))
print("Processed: {}\n\tPrices:{}\n\tErrors:{}".format(package_count, price_count, error_count))
print("\tMisc:{}\n\t1020:{}\n\t1040:{}".format(len(kt_error), len(kt_1020), len(kt_1040)))
fieldnames = ['packageid', 'date', 'tax_profile_id', 'tax_profile', 'occupancy', 'service_level_id', 'service_level', 'code', 'message']

if len(kt_1020) > 0:
    with open('product_launch_1020.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kt_1020)

if len(kt_1040) > 0:
    with open('product_launch_1040.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kt_1040)

if len(kt_error) > 0:
    with open('product_launch_errors.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kt_error)
