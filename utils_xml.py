from xml.etree.ElementTree import ElementTree, Element, SubElement, Comment, tostring
from ElementTree_pretty import prettify
from utils_extractors import get_web, get_highlights, get_services, get_farebase
import logging
from os import path
from time import time
from datetime import datetime
import json
import pickle
import codecs
import logging

logger = logging.getLogger(__name__)

def generate_xml(packages, pricelist, content, departure_types, yearnumber, tax_profile, savepath):
    xml_root = Element('RockyMountaineer')
    xml_products = SubElement(xml_root, 'products', bookingType=tax_profile, date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    package_count = 0
    for p in packages:
        if not p['active']:
            continue

        if p['record_type_name'] != 'Package':
            continue

        package_count += 1
        p_attrib = {
            "name": "{} {}".format(p['external_name'], yearnumber),
            "shortcode": '',
            "code": ''
        }

        # get the core fields...
        packageid = p.get('id')
        logger.debug('=== {} ==='.format(packageid))

        p_attrib['code'] = p.get('product_code')
        if not p_attrib.get('code'):
            p_attrib['code'] = p.get('custom_fields', {}).get('product_code')

        xml_product = SubElement(xml_products, 'product',
                        code=str(p_attrib['code']), 
                        shortcode=str(p_attrib['shortcode']), 
                        name=str(p_attrib['name']))

        p_fields = {}

        try: # default to 2: fixed
            packagetypeid = int(p['departure_type_id'])
            p_fields['packageType'] = departure_types[packagetypeid]
        except:
            p_fields['packageType'] = departure_types[2]

        p_fields['duration'] = str(p.get('length'))
        
        p_fields['departureCity'] = ''
        p_fields['destinationCity'] = ''

        if p['start_location']:
            if 'name' in p['start_location']:
                p_fields['departureCity'] = p.get('start_location', {}).get('name')
        if p['end_location']:
            if 'name' in p['end_location']:
                p_fields['destinationCity'] = p.get('end_location', {}).get('name')
        
        p_fields['routeMap'] = ''
        p_fields['packageImage'] = ''
        for img in p.get('images', []):
            if img['tags'] == 'package-route-map':
                p_fields['routeMap'] = img.get('url')
            elif img['tags'] == 'header-1':
                p_fields['packageImage'] = img.get('url')
        
        p_fields['promotion'] = str(False)
        
        for key, value in p_fields.items():
            logger.debug("\t{} => {}".format(key, value))
            se = SubElement(xml_product, key)
            if len(value) > 0:
                se.text = str(value)

        # prepare the dates for later pruning to a child node
        xml_dates = Element("departureDates")
        if p['dates']:
            for d in p['dates']:
                dd = SubElement(xml_dates, 'date', departureid=d.replace('-',''))
                dd.text = d

        #web_intin = {}
        svc_itin = {}
        highlights = {}

        # prepare the webitinerary
        # prepare the serviceiteinerary
        # prepare the highlights
        content_list = content.get(packageid)
        if content_list:
            content_node = content_list[0].copy()

            web_itin = get_web(content_node)
            svc_itin = get_services(content_node)
            highlights = get_highlights(content_node)

            # web itinierary
            xml_web = SubElement(xml_product, 'webItinerary')
            for w in web_itin:
                xml_wday = SubElement(xml_web, 'day', num=str(w.get('num', '')))
                for w_key, w_value in w.items():
                    if w_key == 'num':
                        continue
                    w_n = SubElement(xml_wday, w_key)
                    w_n.text = str(w_value)
            
            # services
            xml_svc = Element('serviceItinerary')
            for s_key, s_value in svc_itin.items():
                xml_ele = SubElement(xml_svc, 'service', day=str(s_key), itemSequence=str(1), serviceSequence=str(1))
                xml_t = SubElement(xml_ele, 'text', language='English')
                xml_t.text = "<!CDATA[{}]]".format(s_value)
                
            # highlights
            xml_hgh = SubElement(xml_product, 'highlights')
            for hh in highlights:
                x_h = SubElement(xml_hgh, 'highlight')
                x_h.text = str(hh)

        xml_seats = SubElement(xml_product, 'seatClasses')
        service_levels = {}
        for sl in p.get('service_levels', []):
            service_levels[sl.get('name')] = sl
            service_level_id = sl.get('id')

            xml_seat = SubElement(xml_seats, 'seatClass', type= sl.get('name'))
            xml_seat.append(xml_dates)
            
            xml_bases = SubElement(xml_seat, "fareBases")
            if pricelist.get(packageid,{}).get('pricelist'):
                season_prices = get_farebase(pricelist.get(packageid,{}).get('pricelist',{}), tax_profile, packageid, service_level_id)

                # process this dat dict...
                for o_key, o_value in season_prices.items():
                    xml_basis = SubElement(xml_bases, "fareBasis", type=str(o_key))
                    for s_key, s_value in o_value.items():
                        xml_prices = SubElement(xml_basis, "prices", alt="0.00")
                        xml_prices.attrib['from'] = str(s_value['@from'])
                        xml_prices.attrib['to'] = str(s_value['@to'])

                        xml_adult = SubElement(xml_prices, "adult")
                        #pr = s_value.get('prices').copy()

                        x_p = SubElement(xml_adult, 'price')
                        x_p.text = str(s_value['prices']['sales'])

                        x_t = SubElement(xml_adult, 'tax')
                        x_t.text = str(s_value['prices']['tax'])
                    
                    xml_basis.append(xml_svc)

    logger.info("{} exported".format(package_count))

    xml_file = path.join(savepath, 'data', 'webdata-{}-formated.xml'.format(tax_profile.replace(' ', ''))) 
    with codecs.open(xml_file, 'w', encoding='utf8') as fp:
        fp.write(prettify(xml_root))