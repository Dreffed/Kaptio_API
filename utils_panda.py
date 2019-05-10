import pandas as pd 
import logging

logger = logging.getLogger(__name__)

def pivot_pricedata(config, data, kt, savepath):
    if not data:
        data = {}

    if not data.get('price_data'):
        logger.error("missing price data")
        return data

    rows = []
    for t_key, t_value in data.get('price_data').items():
        rows.extend(t_value)

    df = pd.DataFrame(rows)
    print(df.shape)
    print(df.columns.values.tolist())

    df_package = df.groupby([
        'packagedeptype',
        'packagecode',
        'packageid', 
        'packagename', 
        'packagedirection', 
        'packagestart']).size()

    print(df_package.head())
    df_package.to_csv('packagenames_tmp.csv', header=True)


if __name__ == '__main__':
    from utils import get_pickle_data
    data = get_pickle_data('kt_api_data_CAD.pickle')
    pivot_pricedata(None, data, None, None)

