def get_web(content):
    data = {}


    return data

def get_svc(content):
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
    data = {}

    return data
