import pickle
import json
import os
import stat
import sys
import time
import re

# helper functions
def get_pickle_data(pickleName):
    data = {}
    if os.path.exists(pickleName):
        print('Loading Saved Data... [%s]' % pickleName)
        with open(pickleName, 'rb') as handle:
            data = pickle.load(handle)
    return data

def save_pickle_data(data, pickleName):
    print('Saving Data... [%s]' % pickleName)
    with open(pickleName, 'wb') as handle:
        pickle.dump(data, handle)
        
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
        print("ERROR: fileinfo {}".format(e))
        mod_time, acc_time, file_size = ["", "", 0]
        
    return mod_time, acc_time, file_size
        
def scanfiles(folder, filter = None):
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            #print(filename)

            if filter is not None:
                if not isinstance(filter, re.Pattern):
                    filter = re.compile(filter)
                m = re.search(filter, filename)
                if not m:
                    #print("Skipping: {}".format(filename))
                    continue

            try:
                mTime, aTime, fSize = get_fileinfo(filepath)
                
                data= {
                    'folder': dirpath,
                    'file': filename,
                    'modified': mTime,
                    'accessed': aTime,
                    'size': fSize
                }
                yield data
            except Exception as e:
                print("ERROR: scan files failed {}".format(e))


if __name__ == "__main__":
    folder = r"C:\Users\David Gloyn-Cox\OneDrive - Great Canadian Railtour Co\Jupyter_NB" # os.getcwd()
    print(folder)
    filter_str = "[a-zA-Z_]*.json"
    files_str = []
    for f in scanfiles(folder):
        files_str.append(f)

    filter_re = re.compile(filter_str)
    files_re = []
    for f in scanfiles(folder, filter_str):
        files_re.append(f)
        
    print(len(files_str), len(files_re))
    
