from utils import (
        get_pickle_data, save_pickle_data, save_json, 
        scanfiles, load_json
    )
base_name = "kt_api_data"
#pickle_file = "kaptio_allsell.pickle"
pickle_file = "{}.pickle".format(base_name)
data = get_pickle_data(pickle_file)

save_json("{}.json".format(base_name), data)

