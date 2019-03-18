from xml.etree.ElementTree import ElementTree, Element, SubElement, Comment, tostring
import json

def get_farebase(pricelist, tax_profile, packageid, service_level_id):
    
    if not isinstance(pricelist, dict):
        return {}

    fareBasis = {}
    fareDates = set()

    for d_key, d_value in pricelist.items():
        if d_key == "errors":
            continue

        if d_value[tax_profile]:
            if not isinstance(d_value[tax_profile], dict):
                continue

            fareDates.add(d_key)
            for o_key, o_value in d_value[tax_profile].items():
                if not o_key in fareBasis:
                    fareBasis[o_key] = {}
                for p_item in o_value:
                    if p_item['service_level_id'] == service_level_id:
                        if len(p_item['errors']) > 0:
                            continue

                        if p_item['total_price']:
                            if not d_key in fareBasis[o_key]:
                                fareBasis[o_key][d_key] = p_item['total_price'].copy()

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
                prices = f_value[d]
                if prices['sales'] != salesprice:
                    salesprice = prices['sales']
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
                if not 'lastdate' in season_price[d]:
                    season_price[d]['lastdate'] = lastdate  
                sp['@to'] = season_price[d]['lastdate']
                sp['prices'] = season_price[d].copy()
                data[f_key][d] = sp.copy()

    return data
