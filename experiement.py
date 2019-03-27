from kaptiorestpython.client import KaptioClient, load_kaptioconfig
from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import path
from datetime import datetime
from time import time, sleep
import multiprocessing
from multiprocessing.queues import Empty
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def worker_func(in_q, out_q):
    logger.info("A worker has started")    
    w_results = {}
    while not in_q.empty():
        try:
            v = in_q.get(timeout=1)
            packageid = v['id']
            w_results[packageid] = v
        except Empty:
            pass
    out_q.put(w_results)
    out_q.put(None)
    logger.info("A worker has finished")

def worker_pool(package):
    w_results = {}   
    packageid = package['id']
    w_results[packageid] = package
    return w_results

def run_pool(pool_count, packages, option=True):
    p = multiprocessing.Pool(pool_count)
    pool_data = p.map(worker_pool, packages)
    p.close()
    p.join()
    return pool_data

def run_thread(thread_count, in_q, option=True):
    out_q = multiprocessing.Queue()

    workers = []
    for _ in range(thread_count):
        w = multiprocessing.Process(target=worker_func, args=(in_q, out_q,))
        workers.append(w)
        w.start()
    logger.info("Done adding workers")
    
    out_results = {}   
    if option:
        liveprocs = list(workers)
        while liveprocs:
            try:
                while 1:
                    res = out_q.get(False)
                    if res is not None:
                        out_results.update(res)
            except Empty:
                pass

            sleep(0.5)    # Give tasks a chance to put more data in
            if not out_q.empty():
                continue
            liveprocs = [p for p in liveprocs if p.is_alive()]
    else:
        # Collate worker results
        n_proc_end = 0
        while not n_proc_end == thread_count:
            try:
                res = out_q.get()
            except Empty:
                res = None

            if res is None:
                n_proc_end += 1
            else:
                out_results.update(res)

        while True:
            if in_q.empty():
                in_q.close()
                in_q.join_thread()
                break
            logger.info("In Q: {}".format(in_q.qsize()))
            sleep(0.1)

        while True:
            if out_q.empty():
                out_q.close()
                out_q.join_thread()
            logger.info("Out Q: {}".format(out_q.qsize()))
            sleep(0.1)


        # Wait for processes to finish
        for w in workers:
            w.join()
        sleep(0.5)
    return out_results

def main(option=True):

    pickle_file = "kaptio_allsell.pickle"
    data = get_pickle_data(pickle_file)
    kt_packages = data['packages']
    N_PROC = 10

    if option:
        # Input queue to share among processes
        in_q = multiprocessing.Queue()
        for p in kt_packages:
            in_q.put(p)
        logger.info('Added {} packages'.format(len(kt_packages)))

        # Create processes and start them
        out_results = run_thread(N_PROC, in_q, True)
    else:
        out_results = run_pool(N_PROC, kt_packages)

    logger.info("Done join of workers")
    logger.info('Processed {} packages'.format(len(out_results)))

if __name__ == "__main__":
    main(False)



 