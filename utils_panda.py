import pandas as pd 
import logging

logger = logging.getLogger(__name__)

def pivot_pricedata(config, data, kt, savepath):
    if not data:
        data = {}

    if not data.get('price_data')
        logger.error("missing price data")
        return data

    rows = []
    for t_key, t_value in data.get('price_data').items():
        rows.extend(t_value)

    df = pd.DataFrame(rows)

    


