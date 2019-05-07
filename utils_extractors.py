import string
import pickle
import os
import path
import json
from decimal import Decimal
from time import time
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_packagerows(packages):
    rows = []
    for p in packages:
        row = {}
        if p.get('active') != True:
            continue

        row['packageid'] = p.get('id')
        row['packagename'] = p.get('name')
        row['packagelength'] = p.get('length')
        row['packagedeptype'] = p.get('departure_type_id')
        row['packagecode'] = p.get('product_code')
        if not p.get('product_code'):
            row['packagecode'] = p.get('custom_fields',{}).get('product_code')

        row['packagestart'] = 'missing'
        if 'start_location' in p:
            if p.get('start_location') is not None:
                row['packagestart'] = p.get('start_location', {}).get('name')

        rows.append(row)

    logger.info("Found {} packages".format(len(rows)))
    return rows

def get_pricedata(data, rows, tax_profile):
    factor_lookup = {
        "single":1.0,
        "double":2.0,
        "triple":3.0,
        "quad":4.0
    }

    error_list = []
    price_data = []

    service_levels = {}
    if 'service_levels' in data:
        for item in data.get('service_levels',[]):
            sid = item.get('id')
            sname = item.get('name')
            sactive = item.get('active')
            if not sid in service_levels:
                service_levels[sid] = {'name': sname, 'active': sactive}

    for row in rows:
        packageid = row.get('packageid')
        if packageid in data.get('pricelist', {}):
            p_data = data.get('pricelist', {}).get(packageid)
            if not p_data:
                logger.error("Missing price data for {}".format(packageid))
                continue

            if 'pricelist' in p_data:
                for d_key, d_value in p_data['pricelist'].items():
                    if d_key == 'errors':
                        continue

                    # process the date
                    date_str = d_key
                    p_date = datetime.strptime(date_str, "%Y-%m-%d")
                    dow = p_date.strftime('%a')
                    try:
                        e_date = p_date + timedelta(days= row.get('packagelength'))
                    except:
                        e_date = p_date

                    b = {}
                    b['packageid'] = row.get('packageid')
                    b['packagename'] = row.get('packagename')
                    b['packagedeptype'] = row.get('packagedeptype')
                    b['packagecode'] = row.get('packagecode')
                    b['packagestart'] = row.get('packagestart')

                    b['depdate'] = p_date
                    b['arrdate'] = e_date

                    b['mon'] = ('Y' if dow =='Mon' else '')
                    b['tue'] = ('Y' if dow =='Tue' else '')
                    b['wed'] = ('Y' if dow =='Wed' else '')
                    b['thu'] = ('Y' if dow =='Thu' else '')
                    b['fri'] = ('Y' if dow =='Fri' else '')
                    b['sat'] = ('Y' if dow =='Sat' else '')
                    b['sun'] = ('Y' if dow =='Sun' else '')

                    for t_key, t_value in d_value.items():
                        if t_key != tax_profile:
                            continue
                        o_data  = {}
                        for o_key, o_value in t_value.items():
                            # now we are in the array of service types...
                            factor = factor_lookup.get(o_key,1)
                            o_data[o_key] = []

                            for item in o_value:
                                try:
                                    if len(item['errors']) == 0:

                                        if 'total_price' in item:
                                            t = item.get('total_price')
                                            r = {}
                                            r['service_level_id'] = item.get('service_level_id')
                                            r['service_level'] = service_levels.get(item.get('service_level_id'))
                                            r['tax_profile'] = t_key
                                            r["net"] = t.get('net')
                                            try:
                                                r['sales'] = float(t.get('sales'))
                                                r['unitprice'] = float(r['sales']) / factor
                                            except:
                                                r['sales'] = t.get('sales')
                                                r['unitprice'] = r['sales']

                                            r['net_discount'] = t.get('net_discount')
                                            r['sales_discount'] = t.get('sales_discount')
                                            r['tax'] = t.get('tax')
                                            r['currency'] = t.get('currency')
                                            r['supplier_price'] = t.get('supplier_price')
                                            r = {**r}
                                            o_data[o_key].append(r)
                                    else:
                                        for err in item['errors']:
                                            r_err = {}
                                            r_err['packageid'] = row.get('packageid')
                                            r_err['packagename'] = row.get('packagename')
                                            r_err['packagedeptype'] = row.get('packagedeptype')
                                            r_err['depdate'] = d_key
                                            r_err['tax_profile'] = t_key
                                            r_err['service_level_id'] = item.get('service_level_id')
                                            r_err['service_level'] = service_levels.get(item.get('service_level_id'))
                                            r_err['code'] = err.get('error', {}).get('code')
                                            r_err['message'] = err.get('error', {}).get('message')
                                            r_err['details'] = err.get('error', {}).get('details')
                                            error_list.append(r_err)
                                except Exception as ex:
                                    logger.error('Error: {}'.format(ex))
                                    logger.error(json.dumps(row, indent=4))
                                    logger.error(json.dumps(o_value, indent=4))
                        # pivot the occupancy data...
                        # we should have a dict with key of occupancy key and a row for each service level...
                        for s_key, s_value in service_levels.items():
                            s = {}
                            s['service_level_id'] = s_key
                            s['service_level'] = s_value.get('name')
                            for o_key, o_value in o_data.items():
                                for item in o_value:
                                    if item['service_level_id'] == s_key:
                                        # we want this data...
                                        s_prefix = 'xx'
                                        if o_key == 'single':
                                            s_prefix = 'sa'
                                        elif o_key == 'double':
                                            s_prefix = 'da'
                                        elif o_key == 'triple':
                                            s_prefix = 'ta'
                                        elif o_key == 'quad':
                                            s_prefix = 'qa'

                                        s['{}sales'.format(s_prefix)] = item.get('sales')
                                        s['{}net'.format(s_prefix)] = item.get('net')
                                        s['{}tax'.format(s_prefix)] = item.get('tax')
                                        s['{}currency'.format(s_prefix)] = item.get('currency')
                                        s['{}unitprice'.format(s_prefix)] = item.get('unitprice')

                                        s['tax_profile'] = item.get('tax_profile')
                            r = {**b, **s}
                            price_data.append(r)

    logger.info("Rows: {} => Errors: {}".format(len(price_data), len(error_list)))
    
    return {
        "price_data": price_data,
        "errors": error_list
    }

def get_bulkloader_pricedata(data, rows, tax_profile):
    factor_lookup = {
        "single":1.0,
        "double":2.0,
        "triple":3.0,
        "quad":4.0
    }

    service_levels = {}
    if 'service_levels' in data:
        for item in data.get('service_levels',[]):
            sid = item.get('id')
            sname = item.get('name')
            sactive = item.get('active')
            if not sid in service_levels:
                service_levels[sid] = {'name': sname, 'active': sactive}
    
    error_list = []
    price_data = []
    for row in rows:
        packageid = row.get('packageid')
        if packageid in data.get('pricelist', {}):
            p_data = data.get('pricelist', {}).get(packageid)
            if not p_data:
                logger.error("Missing price data for {}".format(packageid))
                continue

            if 'pricelist' in p_data:
                for d_key, d_value in p_data['pricelist'].items():
                    if d_key == 'errors':
                        continue

                    # process the date
                    date_str = d_key
                    p_date = datetime.strptime(date_str, "%Y-%m-%d")
                    dow = p_date.strftime('%a')
                    try:
                        e_date = p_date + timedelta(days= row.get('packagelength'))
                    except:
                        e_date = p_date

                    b = {}
                    b['packageid'] = row.get('packageid')
                    b['packagename'] = row.get('packagename')
                    b['packagedeptype'] = row.get('packagedeptype')
                    b['packagecode'] = row.get('packagecode')
                    b['packagestart'] = row.get('packagestart')

                    b['depdate'] = p_date
                    b['arrdate'] = e_date

                    b['mon'] = ('Y' if dow =='Mon' else '')
                    b['tue'] = ('Y' if dow =='Tue' else '')
                    b['wed'] = ('Y' if dow =='Wed' else '')
                    b['thu'] = ('Y' if dow =='Thu' else '')
                    b['fri'] = ('Y' if dow =='Fri' else '')
                    b['sat'] = ('Y' if dow =='Sat' else '')
                    b['sun'] = ('Y' if dow =='Sun' else '')

                    for t_key, t_value in d_value.items():
                        if t_key != tax_profile:
                            continue
                        o_data  = {}
                        for o_key, o_value in t_value.items():
                            # now we are in the array of service types...
                            factor = factor_lookup.get(o_key,1)                            
                            o_data[o_key] = []

                            for item in o_value:
                                try:
                                    if len(item['errors']) == 0:

                                        if 'total_price' in item:
                                            t = item.get('total_price')
                                            r = {}
                                            r['service_level_id'] = item.get('service_level_id')
                                            r['service_level'] = service_levels.get(item.get('service_level_id'))
                                            r['tax_profile'] = t_key
                                            r["net"] = t.get('net')
                                            try:
                                                r['sales'] = float(t.get('sales'))
                                                r['unitprice'] = float(r['sales']) / factor
                                            except:
                                                r['sales'] = t.get('sales')
                                                r['unitprice'] = r['sales']

                                            r['net_discount'] = t.get('net_discount')
                                            r['sales_discount'] = t.get('sales_discount')
                                            r['tax'] = t.get('tax')
                                            r['currency'] = t.get('currency')
                                            r['supplier_price'] = t.get('supplier_price')
                                            r = {**r}
                                            o_data[o_key].append(r)
                                    else:
                                        for err in item['errors']:
                                            r_err = {}
                                            r_err['packageid'] = row.get('packageid')
                                            r_err['packagename'] = row.get('packagename')
                                            r_err['packagedeptype'] = row.get('packagedeptype')
                                            r_err['depdate'] = d_key
                                            r_err['tax_profile'] = t_key
                                            r_err['service_level_id'] = item.get('service_level_id')
                                            r_err['service_level'] = service_levels.get(item.get('service_level_id'))
                                            r_err['code'] = err.get('error', {}).get('code')
                                            r_err['message'] = err.get('error', {}).get('message')
                                            r_err['details'] = err.get('error', {}).get('details')
                                            error_list.append(r_err)
                                except Exception as ex:
                                    logger.error('Error: {}'.format(ex))
                                    logger.error(json.dumps(row, indent=4))
                                    logger.error(json.dumps(o_value, indent=4))
                        # pivot the occupancy data...
                        # we should have a dict with key of occupancy key and a row for each service level...
                        for s_key, s_value in service_levels.items():
                            s = {}
                            s['service_level_id'] = s_key
                            s['service_level'] = s_value.get('name')
                            for o_key, o_value in o_data.items():
                                for item in o_value:
                                    if item['service_level_id'] == s_key:
                                        # we want this data...
                                        s_prefix = 'xx'
                                        if o_key == 'single':
                                            s_prefix = 'sa'
                                        elif o_key == 'double':
                                            s_prefix = 'da'
                                        elif o_key == 'triple':
                                            s_prefix = 'ta'
                                        elif o_key == 'quad':
                                            s_prefix = 'qa'

                                        s['{}sales'.format(s_prefix)] = item.get('sales')
                                        s['{}unit'.format(s_prefix)] = item.get('person')
                                        s['{}net'.format(s_prefix)] = item.get('net')
                                        s['{}tax'.format(s_prefix)] = item.get('tax')
                                        s['{}unitprice'.format(s_prefix)] = item.get('unitprice')
                                        s['{}currency'.format(s_prefix)] = item.get('currency')

                                        s['tax_profile'] = item.get('tax_profile')
                            r = {**b, **s}
                            price_data.append(r)

    logger.info("Rows: {} => Errors: {}".format(len(price_data), len(error_list)))
    
    return {
        "price_data": price_data,
        "errors": error_list
    }

def get_services(content):
    data = {}
    days = []
    if 'packagedays' in content:
        if isinstance(content['packagedays'], list):
            for item in content['packagedays']:
                day = {}
                day['num'] = str(item.get('packageday_index',0))
                day['dayid'] = item.get('packageday_id', None)
                days.append(day)
    
    events = {}
    if 'packageinformation' in content:
        for info in content.get('packageinformation', []):
            if info.get('packageinfo_category', '') == 'Description':
                text = info.get('packageinfo_text', None)
                dayid = info.get('pacakgeinfo_packageday', None)
                if dayid and text:
                    if not dayid in events:
                        events[dayid] = []
                    events[dayid].append(text)

    for d in days:
        dayid = d.get('dayid', '')
        daynum = d.get('num', 0)
        if not daynum in data:
            data[daynum] = []
        for event in events.get(dayid, []):
            data[daynum].append(event)

    return data

def get_web(content):
    data = []
    if 'packagedays' in content:
        if isinstance(content['packagedays'], list):
            for item in content['packagedays']:
                day = {}
                day['num'] = item.get('packageday_index',0)
                day['Breakfast'] = False
                day['Lunch'] = False
                day['Dinner'] = False

                for infoday in item.get('packageinformation', []):
                    cat = infoday.get('packageinfo_category', '')
                    if cat == 'Description':
                        day['Description'] = infoday.get('packageinfo_text', '')
                    elif cat in ['Breakfast', 'Lunch', 'Dinner']:
                        day[cat] = True

                for locday in item.get('packagedaylocations', []):
                    loc = locday.get('packagedaylocation', {})
                    
                    if locday.get('packagedaylocation_sort', 0) == 1.0:
                        day['StartCity'] = loc.get('location_name', '')
                    elif locday.get('packagedaylocation_sort', 0) == 10.0:
                        day['EndCity'] = loc.get('location_name', '')
                data.append(day)

    return data

def get_highlights(content):
    data = []
    if 'packageinformation' in content:
        for info in content.get('packageinformation', []):
            if info.get('packageinfo_category', '') == 'Activities':
                text = info.get('packageinfo_text', None)
                if text:
                    data.append(text)
    return data

def get_farebase(pricelist, tax_profile, packageid, service_level_id):
    
    if not isinstance(pricelist, dict):
        return {}

    fareBasis = {}
    fareDates = set()
    error_count = 0
    entry_count = 0
    for d_key, d_value in pricelist.items():
        if d_key == "errors":
            continue
        entry_count += 1
        if d_value[tax_profile]:
            if not isinstance(d_value.get(tax_profile), dict):
                continue

            fareDates.add(d_key)
            for o_key, o_value in d_value.get(tax_profile, {}).items():
                if not o_key in fareBasis:
                    fareBasis[o_key] = {}
                for p_item in o_value:
                    if p_item.get('service_level_id') == service_level_id:
                        if len(p_item.get('errors', [])) > 0:
                            error_count += 1
                            continue

                        if p_item.get('total_price'):
                            if not d_key in fareBasis[o_key]:
                                fareBasis[o_key][d_key] = p_item.get('total_price').copy()


    logger.info("\tEntries: {}\n\tErrors: {}".format(entry_count, error_count))
        
    data = {}
    for f_key, f_value in fareBasis.items():
        data[f_key] = {}
        season = set()
        season_price = {}

        salesprice = 0
        keydate = None
        lastdate = None
        for d in sorted(fareDates):
            if d in f_value:
                prices = f_value.get(d)
                if prices.get('sales') != salesprice:
                    salesprice = prices.get('sales')
                    season.add(d)
                    season_price[d] = prices.copy()
                    if keydate:
                        season_price[keydate]['lastdate'] = lastdate
                    keydate = d                        
            lastdate = d
            
        # got the dates, now to build out
        lastdate = sorted(fareDates)[-1]
        
        for d in sorted(season):
            if d in season_price:
                sp = {}
                sp['@from'] = d
                if not 'lastdate' in season_price.get(d):
                    season_price[d]['lastdate'] = lastdate  
                sp['@to'] = season_price.get(d, {}).get('lastdate')
                sp['prices'] = season_price.get(d, {}).copy()
                data[f_key][d] = sp.copy()

    return data