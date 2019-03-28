# load the dependancies
from kaptiorestpython.client import KaptioClient
from kaptiorestpython.utils_kaptio import load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, load_json
from utils_dict import extract_rows
from xml_utilities import get_farebase
from content_utils import get_web, get_svc, get_hgh
from os import path
from time import time
from datetime import datetime
from xml.etree.ElementTree import ElementTree, Element, SubElement, Comment, tostring
from ElementTree_pretty import prettify
import json
import pickle
import codecs
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

homepath = path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = path.join(homepath, *datapaths)
localpaths = ["data", "fresh"]
localpath = path.join(homepath, *localpaths)

kaptio_config_file = path.join(savepath, "config", "kaptio_settings.json")
kaptio_config = load_kaptioconfig(kaptio_config_file)

debug = True
baseurl = kaptio_config['api']['baseurl']

kt = KaptioClient(baseurl, kaptio_config['api']['auth']['key'], kaptio_config['api']['auth']['secret'])

pickle_file = "kaptio_allsell.pickle"
data = get_pickle_data(pickle_file)

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info("Timestamp: {}".format(timestamp))

packageid = 'a754F0000000A30QAE'
tax_profiles = data['tax_profiles']
occupancy = data['occupancy']
search_values = data['search_values']
season_start = data['season']['start']
season_end = data['season']['end']
kt_packages = data['packages']
kt_pricelist = data['pricelist']
kt_content = data['content']

logger.info("Generating XML:\n\t{} packages\n\t{} prices\n\t{} contents".format(
                    len(kt_packages), 
                    len(kt_pricelist), 
                    len(kt_content)))


# bnow to build out the XML
# product code...
# _attributes":{}
# "_fields":{}
# "" : {"_source" : ""}

xmlconfigpath = "xml_fields.json"
fields = load_json(xmlconfigpath)

contentconfigpath = "content_fields.json"
fields = load_json(contentconfigpath)

if 'content' in data:
    kt_content = data['content']

kt_pcontent = {}
for key, value in kt_content.items():

    if isinstance(value, list):
        if not key in kt_pcontent:
            kt_pcontent[key] = []

        for item in value:
            row = extract_rows(item, fields) 
            kt_pcontent[key].append(row)

logger.info("Content {}".format(len(kt_content)))
logger.info("Pivot {}".format(len(kt_pcontent)))

file_path = path.join(savepath, "data", "kt_contents_{}.json".format(timestamp))
save_json(file_path, kt_content)

file_path = path.join(savepath, "data", "kt_pcontent_{}.json".format(timestamp))
save_json(file_path, kt_pcontent)

departure_types = [
    'Anyday', 'Seasonal', 'Fixed'
]

# run though this to get the following:
# webItinerary
# highlights
# 
year = 2020
tax_profile = 'Zero Rated'

xml_root = Element('RockyMountaineer')
xml_products = SubElement(xml_root, 'products', bookingType=tax_profile, date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

package_count = 0
for p in kt_packages:
    if not p['active']:
        continue

    if p['record_type_name'] != 'Package':
        continue
    package_count += 1
    p_attrib = {
        "name": "{} {}".format(p['external_name'], year),
        "shortcode": '',
        "code": ''
    }

    # get the core fields...
    packageid = p['id']
    logger.debug('=== {} ==='.format(packageid))

    if 'custom_fields' in p:
        if 'product_code' in p['custom_fields']:
            p_attrib['code'] = p['custom_fields']['product_code']

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

    p_fields['duration'] = str(p['length'])
    
    p_fields['departureCity'] = ''
    p_fields['destinationCity'] = ''

    if p['start_location']:
        if 'name' in p['start_location']:
            p_fields['departureCity'] = p['start_location']['name']
    if p['end_location']:
        if 'name' in p['end_location']:
            p_fields['destinationCity'] = p['end_location']['name']
    
    p_fields['routeMap'] = ''
    p_fields['packageImage'] = ''
    if 'images' in p:
        for img in p['images']:
            if img['tags'] == 'package-route-map':
                p_fields['routeMap'] = img['url']
            elif img['tags'] == 'header-1':
                p_fields['packageImage'] = img['url']
    
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

    web_intin = {}
    svc_itin = {}
    highlights = {}

    # prepare the webitinerary
    # prepare the serviceiteinerary
    # prepare the highlights
    if packageid in kt_pcontent:
        if len(kt_pcontent[packageid]) > 0:
            content_node = kt_pcontent[packageid][0].copy()

            web_itin = get_web(content_node)
            svc_itin = get_svc(content_node)
            highlights = get_hgh(content_node)

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
    if p['service_levels']:
        for sl in p['service_levels']:
            service_levels[sl['name']] = sl
            service_level_id = sl['id']

            xml_seat = SubElement(xml_seats, 'seatClass', type=sl['name'])
            xml_seat.append(xml_dates)
            
            xml_bases = SubElement(xml_seat, "fareBases")
            if packageid in kt_pricelist:
                if kt_pricelist[packageid]['pricelist']:
                    season_prices = get_farebase(kt_pricelist[packageid]['pricelist'], tax_profile, packageid, service_level_id)

                    # process this dat dict...
                    for o_key, o_value in season_prices.items():
                        xml_basis = SubElement(xml_bases, "fareBasis", type=str(o_key))
                        for s_key, s_value in o_value.items():
                            xml_prices = SubElement(xml_basis, "prices", alt="0.00")
                            xml_prices.attrib['from'] = str(s_value['@from'])
                            xml_prices.attrib['to'] = str(s_value['@to'])

                            xml_adult = SubElement(xml_prices, "adult")
                            pr = s_value['prices'].copy()

                            x_p = SubElement(xml_adult, 'price')
                            x_p.text = str(s_value['prices']['sales'])

                            x_t = SubElement(xml_adult, 'tax')
                            x_t.text = str(s_value['prices']['tax'])
                        
                        xml_basis.append(xml_svc)


logger.info("{} exported".format(package_count))

xml_file = path.join(savepath, 'data', 'webdata-zerorated-formated.xml') 
with codecs.open(xml_file, 'w', encoding='utf8') as fp:
    fp.write(prettify(xml_root))