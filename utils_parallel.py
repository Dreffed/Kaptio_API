# load the dependancies
from kaptiorestpython.client import KaptioClient
from kaptiorestpython.utils_kaptio import load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles, copy_pickles
import json
from time import time
from datetime import datetime
from queue import Queue, Empty
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
        i = 0
        while True:
            # Get the work from the queue and expand the tuple
            p = self.job_queue.get()
            i += 1
            if i % 100 == 0:
                logger.info("Processing {}".format(i))
                
            try:
                packageid = p.get('packageid')
                tax_profiles = p.get('tax_profiles')
                occupancy = p.get('occupancy')
                services = p.get('services')
                dates = p.get('dates')
                currency = p.get('currency', 'CAD')
                channelid = p.get("channelid")

                data = self.kt.process_package_prices(
                        savepath=self.savepath, 
                        packageid=packageid, 
                        dates=dates, 
                        tax_profiles=tax_profiles, 
                        occupancy=occupancy, 
                        services=services,
                        currency=currency,
                        channelid=channelid
                    )

                #self.logger.info(data)
                
                p['pricelist'] = data
                self.result_queue.put(p)

            finally:
                self.job_queue.task_done()    

def process_price_parallel(config, data, kt, savepath):
    if not data:
        data = {}

    job_queue = Queue()
    result_queue = Queue()

    package_field = 'packages'
    key_field = 'package_pricelist'

    logger.info("loading prices...")

    reload = config.get('flags', {}).get('switches', {}).get('reload')
    currency=config.get("presets", {}).get("currency", "CAD")
    try:
        max_threads = int(config.get("presets", {}).get("threads", 5))
    except:
        max_threads = 5

    channelid=None
    for c in data.get("channels",[]):
        if c.get("id") == config.get("presets", {}).get("channelid") or \
                c.get("name") == config.get("presets", {}).get("channelname") or \
                c.get("code") == config.get("presets", {}).get("channelcode"):
            logger.info("Matched channeldata {} => {}".format(c.get("name"), c.get("id")))
            channelid = c.get("id")
            break
        
    if not channelid:
        logger.error("Failed to match channelid {}".format(config.get("presets", {}).get("channelid")))
        raise Exception("Failed to match channelid {}".format(config.get("presets", {}).get("channelid")))
    
    limit_run = int(config.get("presets", {}).get("limit_run",0))
    added = 0
    for p_value in data.get(package_field, []):
        #logger.info("p_value: {}".format(p_value))
        if p_value.get(key_field, []):
            if not reload:
                continue
        p_key = p_value.get('id')

        if not p_value.get('active'):
            if not p_key in config.get('packages',[]):        
                continue
            logger.info('\tIncluding deactive package {}'.format(p_key))
                
        dates = []
        for d in p_value.get('package_dates', []):
            dates.append(d)

        if len(dates) == 0:
            for d in p_value.get('dates', []):
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
            "services": p_value.get('service_levels' ,{}),
            "currency": currency,
            "channelid": channelid
        }

        job_queue.put(run_data)
        if limit_run > 0:
            added += 1
            if added > limit_run:
                logger.info("Run limit hit: {}".format(limit_run))
                break
            
    for _ in range(max_threads):
        worker = ThreadWorker(kt, job_queue, result_queue, savepath)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    # now to wait for the results...
    logger.info("Queue loaded... waiting to finish processing")
    job_queue.join()

    if not data.get('pricelist'):
        data['pricelist'] = {}

    # now to get teh results
    logger.info("Process complete... reading data")
    while True:
    # do stuff with job
        # Get the work from the queue and expand the tuple
        try:
            run_data = result_queue.get(False)
            packageid = run_data.get("packageid")
            if not data.get('pricelist', {}).get(packageid):
                data['pricelist'][packageid] = {}
                    
            data['pricelist'][packageid]['pricelist'] = run_data.get('pricelist')
            
        except Empty:
            break

    logger.info("Processed {} pricelist".format(len(data.get('pricelist'))))

    return data