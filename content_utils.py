import json
import re

def get_svc(content):
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

def get_hgh(content):
    data = []
    if 'packageinformation' in content:
        for info in content.get('packageinformation', []):
            if info.get('packageinfo_category', '') == 'Activities':
                text = info.get('packageinfo_text', None)
                if text:
                    data.append(text)
    return data

def stripUnicode(text):
    q = re.compile(r'\\u([\d]{4})')
    newchr = {
        '2019': "'",
        '2013': "-",
        '2044': "/"
    }
    

        
