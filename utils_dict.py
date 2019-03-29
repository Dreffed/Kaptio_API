import pickle
import json
import os
import shutil
import stat
import sys
import time
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def extract_rows(node, fields):
    """ 
        node - a dictionary of data
        fields an array of dicts
            {
                "source":<>,
                "_output":<>
                "translate": {<term>:<translate>, ...}
            }
    """
    row = {}
    if not isinstance(node, dict):
        return row
    logger.debug("====\n{}".format(fields))
    for key, value in node.items():
        celldata = value
        if key in fields:
            if '_output' in fields[key]:
                outname = fields[key]['_output']
            else:
                outname = key

            if '_fields' in  fields[key]:
                if '_type' in fields[key]:
                    if fields[key]["_type"] == 'list':
                        # this is a list object.
                        celldata = []
                        for item in value:
                            celldata.append(extract_rows(item, fields[key]['_fields']))
                    elif fields[key]["_type"] == 'records':
                        celldata = []
                        for item in value[fields[key]["_type"]]:
                            celldata.append(extract_rows(item, fields[key]['_fields']))
                    elif fields[key]["_type"] == 'dict':
                        celldata = extract_rows(value, fields[key]['_fields'])
            row[outname] = celldata

    return row  