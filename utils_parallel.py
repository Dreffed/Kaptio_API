# load the dependancies
from kaptiorestpython.client import KaptioClient
from kaptiorestpython.utils_kaptio import load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, copy_pickles
import json
from time import time
from datetime import datetime
from queue import Queue
from threading import Thread
import logging

logger = logging.getLogger(__name__)

class ThreadWorker(Thread):
    def __init__(self, kt, job_queue, result_queue, savepath):
        Thread.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.kt = kt
        self.savepath = savepath
        self.job_queue = job_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            p = self.job_queue.get()
            try:
                packageid = p.get('packageid')
                tax_profiles = p.get('tax_profiles')
                occupancy = p.get('occupancy')
                service = p.get('service')
                dates = p.get('dates')

                data = {"done": True} #self.kt.walk_package(self.savepath, packageid, dates, tax_profiles, occupancy, service)
                p['pricelist'] = data
                self.result_queue.put(p)

            finally:
                self.job_queue.task_done()    

def process_packages_parallel(config, data, kt, savepath):
    if not data:
        data = {}
    job_queue = Queue()
    result_queue = Queue()

    package_field = 'packages'
    key_field = 'package_pricelist'

    logger.info("loading prices...")

    for x in range(10):
        worker = ThreadWorker(kt, job_queue, result_queue, savepath)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    reload = config.get('flags', {}).get('switches', {}).get('reload')
    for p_key, p_value in data.get(package_field, {}).items():
        if p_value.get(key_field, []):
            if not reload:
                continue
        if not p_value.get('active'):        
            continue
    
        dates = []
        for d in p_value.get('package_dates', []):
            dates.append(d)

        if len(dates) == 0:
            for d in p_value.get('package_departures', []):
                if d.get('active'):
                    dates.append(d.get('date'))
        run_data = {
            "packageid": p_key,
            "dates": dates,
            "tax_profiles": data.get('tax_profiles', {}),
            "occupancy": data.get('occupancy', {}),
            "services": p_value.get('service_levels' ,{})
        }

        job_queue.put(run_data)
        
    # now to wait for the results...
    job_queue.join()

    

    return data