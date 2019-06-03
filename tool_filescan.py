from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
import csv
import logging
import logging.config
import os

def main():
    logger = logging.getLogger(__name__)

    picklename = 'filelist.pickle'
    data = get_pickle_data(picklename)

    filepath = os.path.expanduser('~')
    filelist = []
    for f in scanfiles(filepath):
        filelist.append(f)

    logger.info('Found {} files'.format(len(filelist)))

    data['files'] = filelist
    fieldnames = ['folder', 'file', 'size', 'modified', 'accessed']

    with open('filelist.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filelist)

    save_pickle_data(data=data, pickleName=picklename)

if __name__ == '__main__':
    main()
