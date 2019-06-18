from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
import csv
import logging
import logging.config
import os
import datetime

logger = logging.getLogger(__name__)

def scanfolder(data, filepath):
    if not data:
        data = {}

    filelist = data.get('files', [])

    scannedpaths = data.get('scanned', {})
    if not filepath in scannedpaths:
        scan = {
            "filepath": filepath,
            "scanned": datetime.datetime.now(),
        }

        found = 0
        for f in scanfiles(filepath):
            filelist.append(f)
            found += 1

        logger.info('Found {} / {} files'.format(found, len(filelist)))
        scan['found'] = found

        data['files'] = filelist
        scannedpaths[filepath] = scan
    
    data['scanned'] = scannedpaths
    return data

def getconnecteddrives():
    import win32api
    drives = win32api.GetLogicalDriveStrings()
    return drives.split('\x00')

def scanstats(data):
    if not data:
        data = {}

    for k,v in data.get('scanned', {}).items():
        logger.info('{} => {}'.format(k, v))

    logger.info('Files: {}'.format(len(data.get('files',[]))))

    return data

def scanfolders(data):
    if not data:
        data = {}

    for drive in getconnecteddrives():
        if drive == '':
            continue
            
        logger.info("scanning {}".format(drive))
        data = scanfolder(data, drive)

    return data

def main(do_scan=None, do_export=None, do_stats=None):
    picklename = 'filelist.pickle'
    data = get_pickle_data(picklename)

    if do_scan:
        data = scanfolders(data)

    if do_stats:
        data = scanstats(data)
        
    if do_export:
        filelist = data.get('files', [])
        if len(filelist) > 0:
            fieldnames = ['folder', 'file', 'size', 'modified', 'accessed']
            with open('filelist.csv', 'w', newline='', encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(filelist)

    save_pickle_data(data=data, pickleName=picklename)

if __name__ == '__main__':
    main(do_export=True)
