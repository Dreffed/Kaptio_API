from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import os

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)

filter_str = "[a-zA-Z_]*.json"

pickle_file = "kaptio_dumps.pickle"
data = get_pickle_data(pickle_file)

data['files'] = []
data['names'] = {}
for f in scanfiles(savepath):
    data['files'].append(f)
    
    if not f['file'] in data['names']:
        data['names'][f['file']] = []
    data['names'][f['file']].append(f)

print("found {} files.".format(len(data['names'])))

save_pickle_data(data, pickle_file)

#savepath = r"c:\Users\dgloyncox\git\dreffed\Kaptio_API"
print("scanning folder {}".format(savepath))
for dirpath, _, filenames in os.walk(savepath):
    print(dirpath, len(filenames))


