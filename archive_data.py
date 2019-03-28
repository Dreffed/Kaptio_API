from utils import get_pickle_data, save_pickle_data, save_json, scanfiles
import json
import pickle
import os
import shutil
import path
from time import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

homepath = os.path.expanduser("~")
datapaths = ["OneDrive - Great Canadian Railtour Co", "Jupyter_NB"]
savepath = os.path.join(homepath, *datapaths)
localpaths = ["data"]
localpath = os.path.join(homepath, *localpaths)

if not os.path.exists(localpath):
    # creeate the folder...
    logger.info("Creating archive directory: {}".format(localpath))
    os.makedirs(localpath)

datapath = os.path.join(savepath, "data")
filecount = 0
for f in scanfiles(datapath, r".*\.json"):
    filecount += 1
    filedate = datetime.strptime(f['modified'], "%Y-%m-%d %H:%M:%S")
    destfolder = os.path.join(localpath, filedate.strftime("%Y-%m-%d"))
    if not os.path.exists(destfolder):
        os.makedirs(destfolder)
    filepath = os.path.join(f.get('folder'), f.get('file'))
    destfile = os.path.join(destfolder, f.get('file'))
    # copy the file over...
    shutil.move(filepath, destfile)
    if filecount % 1000 == 0:
        logger.info("...",)

logger.info("Found {} files.".format(filecount))

