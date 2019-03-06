import time
from datetime import datetime
from kaptiorestpython.helper.http_lib import HttpLib
from kaptiorestpython.helper.exceptions import APIException

def has_empty_warning(result):
    if 'result' not in result \
        and 'warnings' in result \
        and len(result['warnings']) \
        and result['warnings'][0] == 'No assets found for the given search criteria.':
        return True

    return False

