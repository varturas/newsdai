# https://tech.blue-yonder.com/efficient-dataframe-storage-with-apache-parquet/
from qpython import qconnection
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob,traceback,datetime
DATA_PATH='data/pq'

def hdb2pq(yr=2007):
    dfs,ptbl=[None]*4,[None]*4
    with qconnection.QConnection(host='localhost',port=5002,pandas=True) as q:
        dfs[0]=q('select from eod where date within ({}.01.01;{}.03.31)'.format(yr,yr))
        dfs[1]=q('select from eod where date within ({}.04.01;{}.06.30)'.format(yr,yr))
        dfs[2]=q('select from eod where date within ({}.07.01;{}.09.30)'.format(yr,yr))
        dfs[3]=q('select from eod where date within ({}.10.01;{}.12.31)'.format(yr,yr))
    ptbl = [pa.Table.from_pandas(ee,preserve_index=False) for ee in dfs]
    for ii,vv in enumerate(ptbl):
        pq.write_table(ptbl[ii], '{}/df{}Q{}.pq'.format(DATA_PATH,yr,ii+1))

def pq2df(fname=None):
    if fname:df = pq.read_table(fname).to_pandas()
    else:
        dfs=[]
        for fns in sorted(glob.glob('{}/*.pq'.format(DATA_PATH))):
            dfs.append(pq.read_table(fns).to_pandas())
        df = pd.concat(dfs, ignore_index=True)
    try:
        df.columns = [ee.decode('utf-8') for ee in df.columns]
    except:
        traceback.print_exc() 
    df['sym'] = df['sym'].str.decode('utf-8')
    df['date'] = df['date'].dt.date
    return df

def getRetDF(syms=None):
    df = pq2df()
    df = df.sort_values(['sym','date']).reset_index(drop=True)
    df['ret'] = df.groupby('sym')['close'].pct_change()
    # shift weekly return to insure causality
    df['weekRet'] = df.groupby('sym')['close'].shift(1).pct_change(periods=5)
    df['monthRet'] = df.groupby('sym')['close'].shift(1).pct_change(periods=20)
    return df

def find_max_ret(row1):
    row2 = np.array(row1)[~np.isnan(row1)]
    if len(row2)>0:
        return max(row2.min(), row2.max(), key=abs)
    else: return 0

def find_max_idx(ret1,arr1=[]):
    ret2 = np.array(ret1)[~np.isnan(ret1)]
    if len(arr1)>0: arr2 = np.array(arr1)[~np.isnan(ret1)]
    if len(ret2)>0:
        xRet = max(ret2.min(), ret2.max(), key=abs)
        idx = np.where(ret2==xRet)[0][0]
        if len(arr1)>0: return arr2[idx]
        else: return idx
    else:
        if arr1: return arr1[0]
        else: return None

def calc_ret(newsdf,retdf):
    # merging news and return dataframes on date and sym
    resdf=pd.merge(newsdf,retdf,left_on=['symbols','date'],right_on=['sym','date'],how='left',sort=False)
    # groupby headline and date
    grouped=resdf.groupby(['headline','date'])
    resdf = grouped.aggregate(lambda x: tuple(x))
    # find maximum of daily and weekly returns on a column of array of returns and reassign back to the column
    resdf['weekRet'] = resdf.apply(lambda r: find_max_idx(r['ret'],r['weekRet']), axis=1).values
    # find symbol of maximum of daily returns on a column of array of symbols and reassign back to the column
    resdf['maxSym'] = resdf.apply(lambda r: find_max_idx(r['ret'],r['symbols']), axis=1).values
    resdf['gmtstamp'] = resdf.apply(lambda r: find_max_idx(r['ret'],r['gmtstamp']), axis=1).values
    resdf['ret'] = grouped['ret'].apply(lambda x: find_max_ret(x)).values
    # remove index in order to display a dataframe
    resdf.reset_index(inplace=True)
    #resdf=resdf.dropna(subset=['ret'])
    return resdf

def qqq_ret(retdf,newsdf=None):
    # get return of qqq - nasdaq index and assign to ts and indx arrays
    if isinstance(newsdf, pd.DataFrame) and not newsdf.empty:
        qdf=retdf[(retdf['date']>newsdf['date'].min()) & (retdf['date']<=newsdf['date'].max()) & (retdf['sym']=='QQQ')]
    else:
        dt1=datetime.datetime.strptime('2007-01-01', '%Y-%m-%d')
        dt2=datetime.datetime.strptime('2007-12-31', '%Y-%m-%d')
        qdf=retdf[(retdf['date']>dt1.date()) & (retdf['date']<=dt2.date()) & (retdf['sym']=='QQQ')]
    return qdf['date'],(qdf['close']-qdf['close'].mean())

def calc_pnl(df,mname='default'):
    import importlib
    mdl = importlib.import_module('p.pnl.{}'.format(mname))
    # is there an __all__?  if so respect it
    #if "__all__" in mdl.__dict__: names = mdl.__dict__["__all__"]
    names = [x for x in mdl.__dict__ if not x.startswith("_")]
    globals().update({k: getattr(mdl, k) for k in names})
    return pnl(df)

def list_pnl(path='p/pnl'):
    from os import listdir
    from os.path import isfile, join
    files = [f for f in listdir(path) if isfile(join(path, f)) and f != '__init__.py']
    return files
