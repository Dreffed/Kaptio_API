import string
import json
import os
import path
from time import time
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, Color
import logging

logger = logging.getLogger(__name__)

def load_WB(path):    
    # process each sheet...
    # assume top row is the titles
    # subsequent rows are data...
    wb = load_workbook(path)
    return wb

def load_bulkloaderconfig(config_path):
    if not os.path.exists(config_path):
        config = {
            "fieldmap": [
                {
                    "border": "default",
                    "column": "H",
                    "field": "departureCity",
                    "fill": "default",
                    "font": "default",
                    "name": "city"
                }
            ],
            "fills": {
                "bomap": {
                    "fill_type": "solid",
                    "start_color": "F2DCDB"
                },
                "dates": {
                    "fill_type": "solid",
                    "start_color": "F2DCDB"
                },
                "default": {
                    "fill_type": "solid",
                    "start_color": "FFFFCC"
                },
                "dentry": {
                    "fill_type": "solid",
                    "start_color": "BFBFBF"
                },
                "kids": {
                    "fill_type": "solid",
                    "start_color": "FFCC99"
                },
                "lentry": {
                    "fill_type": "solid",
                    "start_color": "E6E6E6"
                },
                "map": {
                    "fill_type": "solid",
                    "start_color": "E6E6F1"
                }
            },
            "fonts": {
                "default": {
                    "name": "Ariel",
                    "size": 18
                },
                "formula": {
                    "italic": True,
                    "name": "Ariel",
                    "size": 18
                }
            },
            "sheetname": "2018",
            "sides": {
                "default": {
                    "color": "B2B2B2",
                    "style": "thin"
                }
            },
            "start_row": 9
        }
        with open(config_path, 'w') as outfile:
            json.dump(config, outfile, indent=4)
        
    else:
        # load the settings...
        config = json.load(open(config_path))

    logger.info("Loaded bulkloader config data...")
    return config

def generate_bulkloader(price_data, savepath, template, yearnumber, versionnumber, tax_profile, config, currency="CAD"):
    excel_feed_path = os.path.join(savepath, 'templates', template)
    bulk_file_name = 'Rocky Bulk Loader.{}.{}.{}.{}.xlsx'.format(tax_profile.replace(" ", ""), yearnumber, versionnumber, currency)

    row_idx = config['start_row']-1

    logger.info('=== Styles ===')
    style_fills = {}
    if 'fills' in config:
        logger.info('\t=== Files ===')
        for key in config['fills']:
            logger.info('\t\t{} -> {}'.format(key, config['fills'][key]))
            style_fills[key] = PatternFill(**config['fills'][key])
            logger.debug(style_fills[key])

    style_fonts = {}
    if 'fonts' in config:
        logger.info('\t=== Fonts ===')
        for key in config['fonts']:
            logger.info('\t\t{} -> {}'.format(key, config['fonts'][key]))
            style_fonts[key] = Font(**config['fonts'][key])
            logger.debug(style_fonts[key])

    style_sides = {}
    if 'sides' in config:
        logger.info('\t=== Sides ===')
        for key in config['sides']:
            logger.info('\t\t{} -> {}'.format(key, config['sides'][key]))
            style_sides[key] = Side(**config['sides'][key])
            logger.debug(style_sides[key])

    wb = load_WB(excel_feed_path)
    ws = wb[config['sheetname']]
    ws.title = "{}".format(yearnumber)

    for row in price_data:
        row_idx += 1 
        for field in config['fieldmap']:
            coord = '{}{}'.format(field['column'], row_idx)
        
            # format the field..
            # format the field..
            key_name = 'fill'
            if key_name in field:
                ws[coord].fill = style_fills[field[key_name]]
                
            key_name = 'font'
            if key_name in field:
                ws[coord].font = style_fonts[field[key_name]]

            key_name = 'border'
            if key_name in field:
                cSide = style_sides[field[key_name]]
                ws[coord].border = Border(left=cSide , right=cSide, top=cSide, bottom=cSide)
                
            # condition is a check for NONE in the specified field is none skip that record...
            clear_cell = False
            if 'condition' in field:
                condition_field = field['condition']
                
                if condition_field in row:
                    if row[condition_field] is None or row[condition_field] == '':
                        clear_cell = True
                else:
                    clear_cell = True

            if clear_cell:
                ws[coord].value = None
                continue
                
            if 'field' in field:
                key = field['field']
                if key in row:
                    if 'format' in field and 'type' in field:
                        field_type = field['type']
                        if field_type == 'date':
                            ws[coord].value = row[key].strftime(field['format'])
                        else:
                            ws[coord].value = row[key]
                    else:    
                        ws[coord].value = row[key]
                        
            elif 'formula' in field:
                formula = field['formula']
                
                formula = formula.replace('{r}', str(row_idx))
                ws[coord].value = formula
            
            elif 'equation' in field:
                pass
                
            elif 'value' in field:
                ws[coord].value = field['value']
                        
    excel_save_path = os.path.join(savepath, 'data', bulk_file_name)
    wb.save(excel_save_path)

    logger.info('Document saved... {}'.format(excel_save_path))