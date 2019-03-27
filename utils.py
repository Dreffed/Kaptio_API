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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# helper functions
def get_pickle_data(pickleName):
    data = {}
    if os.path.exists(pickleName):
        logger.info('Loading Saved Data... [%s]' % pickleName)
        with open(pickleName, 'rb') as handle:
            data = pickle.load(handle)
    return data

def save_pickle_data(data, pickleName):
    logger.info('Saving Data... [%s]' % pickleName)
    with open(pickleName, 'wb') as handle:
        pickle.dump(data, handle)

def load_json(filepath):
    with open(filepath, 'r') as fp:
        data = json.load(fp)
    return data

def save_json(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def get_fileinfo(filename):
    time_format = '%Y-%m-%d %H:%M:%S'
    try:
        file_stats = os.stat(filename)
        mod_time = time.strftime(time_format, time.localtime(file_stats[stat.ST_MTIME]))
        acc_time = time.strftime(time_format, time.localtime(file_stats[stat.ST_ATIME]))
        file_size = file_stats[stat.ST_SIZE]
        
    except Exception as e:
        logger.info("ERROR: fileinfo {}".format(e))
        mod_time, acc_time, file_size = ["", "", 0]
        
    return mod_time, acc_time, file_size
        
def scanfiles(folder, filter = None):
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            logger.debug(filename)

            m_data = None
            if filter is not None:
                if not isinstance(filter, re.Pattern):
                    filter = re.compile(filter)
                m = re.search(filter, filename)
                if not m:
                    logger.debug("Skipping: {}".format(filename))
                    continue

                m_data = [m.groupdict() for m in filter.finditer(filename)]

            try:
                mTime, aTime, fSize = get_fileinfo(filepath)
                
                data= {
                    'folder': dirpath,
                    'file': filename,
                    'modified': mTime,
                    'accessed': aTime,
                    'size': fSize
                }
                if m_data:
                    data['matches'] = m_data

                yield data
            except Exception as e:
                logger.info("ERROR: scan files failed {}".format(e))

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

def copy_pickles(savepath):
    import socket
    hostname = socket.gethostname()

    data = {}

    for f in scanfiles('.', r'.*\.pickle'):
        fpart = f['file'].split('.')
        if len(fpart) != 2:
            continue
        try:
            fdate = datetime.strptime(f['modified'], '%Y-%m-%d %H:%M:%S')
            newname = '{}.{}.{}.{}'.format(fpart[0], hostname, fdate.strftime('%Y%m%d%H%M'), fpart[-1])
            if not os.path.exists(newname):
                logger.info("Creating copy: {} => {}".format(f['file'], newname))
                shutil.copy(f['file'], newname)
            dstpath = os.path.join(savepath, 'config', newname)

            if not os.path.exists(dstpath):
                logger.info("Copy to share: {} => {}".format(f['file'], newname))
                shutil.copy(f['file'], dstpath)

            data[f.get('file'), 'file'] = {
                'src': f.get('file'),
                'copy':newname,
                'dest':dstpath
            }
        except Exception as ex:
            logger.error("Failed to copy file {} {}".format(f.get('file'), ex))

    return data

def scan_packagefiles(savepath):
    data = {}
    for f in scanfiles(os.path.join(savepath, 'data'), r"kt_package_(?P<packageid>[a-zA-Z0-9]+)_(?P<datestr>[\d]+).*.json"):
        
        for m in f.get('matches', []):
            packageid = m.get('packageid', None)

        if packageid and not data.get(packageid, None):
            try:
                json_data = load_json(os.path.join(f.get('folder'), f.get('file')))
                data[packageid] = json_data
                data[packageid]['file'] = f.copy()

            except Exception as ex:
                logger.info("skipping file {}\n{}".format(f, ex))
    return data

if __name__ == "__main__":
    import socket
    hostname = socket.gethostname()
    homepath = os.path.expanduser("~")
    datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
    savepath = os.path.join(homepath, *datapaths)
    scan_packagefiles(savepath, )

    copypickle = True

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("Timestamp: {}".format(timestamp))
    if copypickle:
        copy_pickles(savepath)
    else:
        kt_processed = scan_packagefiles(savepath)
        logger.info(len(kt_processed))
        filepath = os.path.join(savepath, "data", "kt_preocessed__{}.json".format(timestamp))
        save_json(filepath, kt_processed)



