import pickle
import json
import os

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