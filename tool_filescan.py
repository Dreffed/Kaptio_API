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

def exportfiles(data):
    filelist = data.get('files', [])
    if len(filelist) == 0:
        logger.info('No files to export')
        return data

    fieldnames = filelist[0].keys()

    with open('filelist.csv', 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filelist)

def processfilelist(data):
    if not data:
        data = {}

    filelist = data.get('files', [])
    extlist = data.get('extensions', {})
    keywords = data.get('words', {})

    if len(filelist) > 0 and not 'ext' in filelist[0]:
        for f in filelist:
            filename, ext = os.path.splitext(f.get('file'))
            f['ext'] = ext
            f['filename'] = filename

            filepath = os.path.join(f.get('folder'), f.get('file'))
            if not ext in extlist:
                extlist[ext] = []
            extlist[ext].append(filepath)

    data['extensions'] = extlist
    return data

def main(processlist):
    picklename = 'filelist.pickle'
    data = get_pickle_data(picklename)

    processmap = buildprocessmap()

    for p in processlist:
        proc = processmap.get(p)
        if proc:
            logger.info('Running {}'.format(p))
            data = proc(data)

    save_pickle_data(data=data, pickleName=picklename)

def buildprocessmap():
    processmap = {
        'scan': scanfolders,
        'process': processfilelist,
        'stats': scanstats,
        'export': exportfiles
    }
    return processmap

if __name__ == '__main__':
    processlist = [
        'scan',
        'process',
        'stats',
        'export',
        'null'
    ]

    main(processlist)
