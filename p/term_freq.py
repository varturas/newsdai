#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
import sys, os, traceback
from argparse import ArgumentParser
import logging as log
from lib import *
import newsdai_mktmv as nmkt
import pandas as pd
import numpy as np
import re,socket,datetime
from sklearn.neighbors.kde import KernelDensity
from scipy.signal import argrelextrema
import signal
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mds
import seaborn as sns

log.getLogger().setLevel(log.WARN)

class TF:
    dataPath = "../data"
    tfFile = dataPath + "/termFreq"
    tfFileCsv = tfFile + ".csv"
    tfFilePkl = tfFile + ".pkl"

    def __init__(self, args):
        self.args = args
        self._pos = 0
        self.nrows = 5
        self.file = self.args.view
        self._df = pd.read_pickle(self.file)
        indxFile = TF.dataPath+'/out/indxSPYDF_2005-01-03-2008-12-30.pkl'
        self.ixDF = pd.read_pickle(indxFile)
        sns.set_style("darkgrid")

    def on_event(self, e):
        log.debug("got a key event!")
        if log.DEBUG<=log.getLogger().getEffectiveLevel():
            print(e); sys.stdout.flush()
        if e.button:
            if e.button == 3:
                nLen = len(self._df.index)
                if self._pos<nLen-1: self._pos += self.nrows
                else: self._pos = nLen-1
                log.debug("step forward to pos:%d" % (self._pos))
            elif e.button == 1 and e.dblclick:
                if self._pos>=1: self._pos -= self.nrows
                else: self._pos=0
                log.debug("step back to pos:%d" % (self._pos))
            else:
                return
        else:
            return
        self.viewAt(self._pos)

    def viewAt(self, pos=None):
        import warnings
        warnings.filterwarnings('ignore')
        warnings.simplefilter(action='ignore', category=FutureWarning)
        if not pos: pos=self._pos
        for ii in range(self.nrows):
            self.ax[ii].clear()
            if pos+ii>=len(self._df.index): continue
            vv = self._df.iloc[pos+ii].tdf.tolist()
            tag = self._df.iloc[pos+ii].tag
            df = pd.DataFrame(vv, columns=['y'])
            #df['x'] = range(len(vv))
            df['x'] = self._df.iloc[pos+ii].tdt.tolist()
            if ii<self.nrows-1: self.ax[ii].set_xticklabels([])
            sns.lineplot(x='x', y='y', data=df, label=tag, ax=self.ax[ii])
            y_min,y_max = df['y'].min(),df['y'].max()
            p_min,p_max = self.ixDF['price'].min(),self.ixDF['price'].max()
            self.ixDF['price'] = (self.ixDF.price - p_min) / (p_max - p_min) * (y_max - y_min) + y_min
            sns.lineplot(x='date', y='price', data=self.ixDF, color='tan', ax=self.ax[ii])
            self.ax[ii].set_ylabel(tag)    
            self.ax[ii].set_xlabel('')
            self.ax[ii].legend(loc='upper right').set_title(tag)
        self.ax[self.nrows-1].xaxis.set_major_formatter(mds.DateFormatter('%Y%m%d'))
        plt.setp(self.ax[self.nrows-1].get_xticklabels(), visible=True, rotation=45)
        plt.draw()

    def viewTerm(self):
        tags = self.args.viewTerm.split(',')
        fig,ax = plt.subplots(nrows=len(tags),ncols=1,figsize=(20, 10),squeeze=True)
        for ii,tag in enumerate(tags):
            if isinstance(ax, list) or type(ax) is np.ndarray: lax = ax[ii]
            else: lax = ax
            lax.clear()
            vv = self._df.loc[self._df.tag==tag,['tdf']].values.flatten()[0].tolist()
            vvv = [np.nan if ee==0 else ee for ee in vv]
            df = pd.DataFrame(vvv, columns=['y'])
            xx = range(len(vv))
            df['x'] = tt = self._df.loc[self._df.tag==tag,['tdt']].values.flatten()[0].tolist()
            coeffs = np.polyfit(xx, vv, deg=4)
            poly = np.poly1d(coeffs)
            yp = np.polyval(poly, xx)
            pDF = pd.DataFrame(yp, columns=['yp'])
            pDF['x']  = tt
            if ii<len(tags)-1: lax.set_xticklabels([])
            y_min,y_max = df['y'].min(),df['y'].max()
            sns.lineplot(x='x',y='y',data=df,ax=lax,hue=df['y'].isna().cumsum(),palette=['blue']*(1+sum(df['y'].isna())),legend=False)
            lax.legend().set_title(tag)
            p_min,p_max = self.ixDF['price'].min(),self.ixDF['price'].max()
            self.ixDF['price'] = (self.ixDF.price - p_min) / (p_max - p_min) * (y_max - y_min) + y_min
            sns.lineplot(x='date', y='price', data=self.ixDF, color='tan', ax=lax)
            yp1 = sns.lineplot(x='x', y='yp', data=pDF, color='red', ax=lax)
            yp1.axhline(12*np.std(vv[:len(vv)//2]),color='k',linestyle='--')
            yp1.axhline(6*np.std(vv[:len(vv)//2]),color='g',linestyle='-.')
            lax.set_ylabel(tag)    
            lax.set_xlabel('')
        if isinstance(ax, list) or type(ax) is np.ndarray: lax = ax[ii]
        else: lax = ax
        lax.xaxis.set_major_formatter(mds.DateFormatter('%Y%m%d'))
        plt.setp(lax.get_xticklabels(), visible=True, rotation=45)
        plt.draw()

    def view(self):
        if hasattr(self.args,'viewTerm') and self.args.viewTerm: self.viewTerm()
        else:
            fig,ax = plt.subplots(nrows=self.nrows,ncols=1,figsize=(10, 6),squeeze=True)
            self.ax = ax
            fig.canvas.mpl_connect('button_press_event', self.on_event)
            self.fig = fig
            self.viewAt()
        plt.show(block=True)


def find_clust(vv):
    clust = []
    a = np.array(vv).reshape(-1, 1)
    kde = KernelDensity(kernel='gaussian', bandwidth=3).fit(a)
    s = np.linspace(0,5*len(vv))
    e = kde.score_samples(s.reshape(-1,1))
    mi, ma = argrelextrema(e, np.less)[0], argrelextrema(e, np.greater)[0]
    for ii in range(len(mi)):
      if ii==0:
        clust.append(a[a<mi[ii]].reshape(-1))
      elif ii<len(mi):
        clust.append(a[(a>=mi[ii-1])*(a<mi[ii])].reshape(-1))
        if ii==len(mi)-1:
            clust.append(a[a>mi[ii]].reshape(-1))
    return clust

def run_term_analysis(mv):
    log.warn('running term freq analysis')
    #tagDF = pd.read_csv(TF.dataPath+'/test.csv', sep=',')
    tagDF = pd.read_csv(TF.dataPath+'/tags.csv', sep=',')
    tfDF = pd.DataFrame(columns=['tag','count','delta','q1','q2','q3','q4','q5','tot','min','avg','max','std','cdelta','nclust','tdf'])
    for ii,tt in enumerate(tagDF['tag'].tolist()):
        try:
            tdf = mv.getTermFreq(query=tt)
            clust = find_clust(tdf.tfreq.tolist())
            lvl = [np.median(cc) for cc in clust if len(cc)>0 and np.median(cc)>0]
            if lvl and len(lvl)>0: cdelta = lvl[-1] - lvl[0]
            else: cdelta = 0.0
            nclust = len(clust)
            st = tdf.groupby(pd.qcut(tdf.tfreq.rank(method='first'), 5, labels=False, duplicates='drop')).agg(['median','std','sum'])
            q1,q2,q3,q4,q5,tot = st['tfreq'].iloc[0]['median'],st['tfreq'].iloc[1]['median'],st['tfreq'].iloc[2]['median'],st['tfreq'].iloc[3]['median'],st['tfreq'].iloc[4]['median'],st['tfreq'].iloc[0]['sum']+st['tfreq'].iloc[1]['sum']+st['tfreq'].iloc[2]['sum']+st['tfreq'].iloc[3]['sum']+st['tfreq'].iloc[4]['sum']
            tfDF = tfDF.append({'tag':tt,'count':len(tdf.index),'delta':q5-q2,'q1':q1,'q2':q2,'q3':q3,'q4':q4,'q5':q5,'tot':tot,'min':np.min(tdf.tfreq),'avg':np.mean(tdf.tfreq),'max':np.max(tdf.tfreq),'std':np.std(tdf.tfreq),'cdelta':cdelta,'nclust':nclust,'tdf':tdf.tfreq,'tdt':tdf.date},ignore_index=True)
            if ii%10==1: print('processed {}, term: {}'.format(ii,tt),end='\r',flush=True)
        except:
            log.error('error in processing term:'.format(tt))
            traceback.print_exc()
    tfDF['rank'] = tfDF['delta']/tfDF['tot']
    tfDF = tfDF.sort_values('rank',ascending=False)
    tfDF.to_pickle(TF.tfFilePkl)
    tfDF.to_csv(TF.tfFileCsv, sep=',', columns=['tag','count','delta','q1','q2','q3','q4','q5','tot','min','avg','max','std','cdelta','nclust'], mode='w', index=False)

def main(args):
    if hasattr(args,'dates') and args.dates:
        args.dates = [(datetime.datetime.strptime(dd,'%Y%m%d')).date() for dd in args.dates.split('-')]
    else: args.dates = None
    if hasattr(args,'dates') and args.dates and hasattr(args,'term') and args.term is not None:
        mktmv = nmkt.MktMoveToNews(args)
        print(mktmv.getTermFreq(dates=args.dates,query=args.term))
    elif hasattr(args,'analysis') and args.analysis:
        mktmv = nmkt.MktMoveToNews(args)
        if not args.dates: args.dates = [datetime.datetime(2005,3,23),datetime.datetime(2008,3,30)]
        mktmv.setDates(args.dates)
        run_term_analysis(mktmv)
    elif hasattr(args,'view') and args.view:
        if not args.view: args.view=TF.tfFilePkl 
        tfObj = TF(args)
        tfObj.view()
    else: parser.print_help()

def signal_handler(signal, frame):
    log.warn('Caught Ctrl+C!')
    try:
        sys.exit(1)
    except SystemExit:
        os._exit(0)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-D', action='store', dest='dates', help='start and end dates')
    parser.add_argument('-t', action='store', dest='term', nargs='?', default=None, const=None, help='run term freq analysis for a term')
    parser.add_argument('-T', action='store', dest='viewTerm', help='view term frequency for a collection of tags')
    parser.add_argument('-v', action='store', dest='view', nargs='?', default=None, const=TF.tfFilePkl, help='view term freq file')
    parser.add_argument('-a', action='store_true', dest='analysis', help='run term freq analysis on all terms')
    args = parser.parse_args()
    signal.signal(signal.SIGINT, signal_handler)
    try:
        main(args)
    except KeyboardInterrupt:
        print('Caught Keyboard Interrupt')
        sys.exit(1)
    sys.exit(0)

