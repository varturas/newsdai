#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
import sys, os, traceback
from argparse import ArgumentParser
import logging as log
from lib import *
import pandas as pd
import datetime
import qpython
from qpython import qconnection

log.getLogger().setLevel(log.WARN)

class Args:
    def __init__(self):
        pass

# run results clustering
class SolrCluster:
    def __init__(self, args=Args):
        self.args = args
        if hasattr(args,'news'): self.newsQ = args.news
        else: self.newsQ = {}
        self.news = []
        self.clusters = []
        self.maxRows = 2147483647
        self.solrUrl = 'localhost:8983'
        self.count = 0

    def run(self):
        news = self.newsQ
        # divide news into 2 sets: one with empty Headlines, and the second with non-empty Headlines
        idss = [[],[]]
        idss[0] = [ee for tt in news.iterrows() if tt[1]['headline']==b'' for ee in tt[1]['prevID'].decode().split('|') if ee]
        idss[1] = [ii for ii in [tt[1]['newsID'].decode() for tt in news.iterrows() if tt[1]['headline']!=b''] if ii not in idss[0]]
        #ids = [ii.decode() for ii in news['newsID'].tolist()]
        solrClust = [[],[]]
        qurl = 'http://' + self.solrUrl + '/solr/newsdai/clustering'
        for ii,ids in enumerate(idss):
            if len(ids)==0:
                log.error('no news IDs to query')
                continue
            if len(ids)>999: ids=ids[:1000]
            qstr = 'md5ID:('+' '.join(ids)+')'
            log.debug('1st solr query qstr:{}'.format(qstr))
            res2 = self.query(qurl, qstr)
            if not res2 or len(res2)<2 or not 'clusters' in res2[1]:
                log.error('malformed clusters response')
                continue
            for jj,rr in enumerate(res2[1]['clusters']):
                try:
                    rr['labels'] = rr['labels'][0]
                    solrClust[ii].append(rr)
                except:
                    log.error('error unpacking result for query:{}'.format(qstr))
                    traceback.print_exc()
        self.clusters = solrClust

    def getQueryRes(self, queryStr):
        qstr = 'Headline:"'+queryStr+'"~100'
        qurl = 'http://' + self.solrUrl + '/solr/newsdai/select'
        solrDF,rawRes = self.query(qurl, qstr)
        return solrDF

    def query(self, qurl, qstr):
        solrRes,res = [],None
        try:
            import urllib,requests
            qq = { "params": {"q": qstr, "wt": "json", "rows": str(self.maxRows) } }
            resp = requests.post(qurl, json=qq)
            if resp: res=resp.json()
        except:
            log.error('error in solr query:{}'.format(qstr))
            traceback.print_exc()
        if not res:
            log.error('result is empty for query:{}'.format(qstr))
            return
        self.count = int(res['response']['numFound'])
        log.warn('Number of results found in solr, numFound: {}'.format(self.count))
        if self.count==0:
            log.error('no results found')
            return
        if not 'response' in res or not 'docs' in res['response']:
            log.error('malformed response json')
            return
        for rr in res['response']['docs']:
            try:
                rr['headline'] = rr['Headline']
                rr['prevHeadline'] = ""
                if 'GmtTimeStamp' not in rr.keys() or len(rr['GmtTimeStamp'])==0: continue
                tt = datetime.datetime.strptime(rr['GmtTimeStamp'],'%Y-%m-%dT%H:%M:%S.000000000')
                rr['gmtstamp'] = (tt-datetime.datetime(1970,1,1)).total_seconds()
                rr['date'] = tt
                if 'ret' in rr and len(rr['ret'])>0: 
                    retMax = max(rr['ret'], key=abs) 
                    if retMax != -9.9E-4: rr['ret'] = retMax
                    else: rr['ret'] = float("nan")
                else: rr['ret'] = float("nan")
                if 'CompanyCodes' in rr:
                    rr['sym'] = ','.join(rr['CompanyCodes'])
                else:
                    rr['sym'] = "QQQ"
                solrRes.append(rr)
            except:
                log.error('error unpacking result for query:{}'.format(qstr))
                traceback.print_exc()
        return solrRes,res

    def getNews(self):
        return self.news

    def getClusters(self):
        return self.clusters

class MktMoveToNews:
    def __init__(self, args=Args):
        self.args = args
        try:
            self.q = qconnection.QConnection(host = 'localhost', port = 5001, pandas = True)
        except:
            self.q = None
            log.warn("cannot establish q connection, setting to None")
            if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
        self.qquery = None; self.newsDict = None; self.kwords = None; self.dates = None
        self.solrNews = None
        self.news = pd.DataFrame(columns=['sym','headline','prevHeadline','gmtstamp','date','ret'])

    def setFunction(self, qquery):
        self.args.qquery = qquery
        self.args.dates = None

    def setDates(self, dates):
        self.args.dates = dates
        self.args.qquery= None

    def setSymbols(self, symbols):
        self.args.symbols = symbols

    def setKeywords(self, searchStr):
        kwords = searchStr.split()
        if len(kwords)==1: self.args.kwords = '"'+kwords[0]+'"'
        else: self.args.kwords = '("'+'";"'.join(searchStr.split())+'")'

    def exec_q(self):
        args = self.args
        if not self.q:
            log.error("cannot execute q query")
            return None
        try:
            if args and hasattr(args, 'qquery') and args.qquery:
                self.qquery = args.qquery
            else:
                if args and hasattr(args, 'dates') and args.dates:
                    dateStr = [datetime.datetime(dt.year, dt.month, dt.day).strftime('%Y-%m-%d') for dt in args.dates]
                    if len(dateStr) == 0: raise Exception('wrong dates format')
                    elif len(dateStr) == 1: self.dates = '"D"$"{}"'.format(dateStr[0])
                    else: self.dates = '("D"$"{}";"D"$"{}")'.format(dateStr[0],dateStr[1])
                else: raise Exception('no dates in the arguments')
                if args and hasattr(args, 'symbols') and args.symbols:
                    symbols = args.symbols.replace(',','`')
                    self.symbols = '`{}'.format(symbols)
                else: self.symbols = "`"
                if args and hasattr(args, 'kwords') and args.kwords:
                    self.kwords = args.kwords
                else: self.kwords = "`"
                self.qquery = 'mktMove2News[{};{};{}]'.format(self.kwords,self.dates,self.symbols)
            self.q.open()
            log.warn('executing market impact query:{}'.format(self.qquery))
            self.news = self.q.sendSync(self.qquery)
        except:
            log.error('error in q query')
            traceback.print_exc()
            if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
        self.news = self.news.rename(columns={'Headline':'headline','GmtTimeStamp':'gmtstamp'})

    # run result clustering and output results
    def find_solrClusters(self):
        self.args.news = self.news
        if not self.solrNews: self.solrNews = SolrCluster(self.args)
        self.solrNews.run()

    def findSolrNews(self, qStr):
        log.warn('running solr query:{}'.format(qStr))
        if not self.solrNews: self.solrNews = SolrCluster(self.args)
        solrNews = self.solrNews.getQueryRes(qStr)
        return  pd.DataFrame({ \
                'sym':pd.Series([dd['sym'] for dd in solrNews], dtype='object'), \
                'headline':pd.Series([dd['headline'] for dd in solrNews], dtype='str'), \
                'prevHeadline':pd.Series([dd['prevHeadline'] for dd in solrNews], dtype='str'), \
                'gmtstamp':pd.Series([dd['gmtstamp'] for dd in solrNews], dtype='float'), \
                'date':pd.Series([dd['date'] for dd in solrNews], dtype='datetime64[ns]'), \
                'ret':pd.Series([dd['ret'] for dd in solrNews], dtype='float')
            })


    def getNewsFromQ(self, news):
        gmtstamps = []
        tt0 = datetime.datetime(1970,1,1)
        for g1 in news['gmtstamp'].tolist():
            if g1 == b'': dt = 0
            else:
                tt = datetime.datetime.strptime(g1.decode(),'%Y.%m.%dD%H:%M:%S.000000000')
                dt = (tt-tt0).total_seconds()
            gmtstamps.append(dt)
        syms = [ss.decode() for ss in news['sym'].tolist()]
        return pd.DataFrame({ \
                'sym':pd.Series(syms, dtype='object'), \
                'headline':pd.Series(news['headline'].tolist(), dtype='str'), \
                'prevHeadline':pd.Series(news['prevHeadline'].tolist(), dtype='str'), \
                'gmtstamp':pd.Series(gmtstamps, dtype='float'), \
                'date':pd.Series(news['date'].tolist(), dtype='datetime64[ns]'), \
                'ret':pd.Series(news['ret'].tolist(), dtype='float')
            })

    def findMktNews(self):
        self.exec_q()
        return self.getNewsFromQ(self.news)

    def getSolrNews(self):
        return self.solrNews.getNews()

    def getClusters(self):
        return self.solrNews.getClusters()

    def getQuery(self):
        return self.qquery

    def setQuery(self, qquery):
        self.qquery = qquery

    def printNewsDF(self, lim=-1):
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', -1):
            if lim > 0:
                #print(self._news[['headline', 'symbols']])
                print(self.news.iloc[:lim])

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-D', action='store', dest='dates', help='start and end dates')
    parser.add_argument('-k', action='store', dest='kwords', help='specify keywords')
    parser.add_argument('-s', action='store', dest='symbols', help='specify symbols')
    parser.add_argument('-t', action='store', dest='test', nargs='?', default=None, const='', help='run unit tests')
    args = parser.parse_args()
    if hasattr(args,'dates') and args.dates:
        args.dates = [datetime.datetime.strptime(dd,'%Y%m%d') for dd in args.dates.split('-')]
    mktmv = MktMoveToNews(args)
    # example: ./newsdai_mktmv.py -D 20070202-20070204 -t
    if len(sys.argv)>1 and hasattr(args,'dates') and hasattr(args,'test'):
        if args.test and len(args.test)>0:
            print(mktmv.findSolrNews(args.test))
        elif args.dates and len(args.dates)>0:
            print(mktmv.findMktNews())
            mktmv.find_solrClusters()
        else:
            parser.print_help()
            print("Example: ./newsdai_mktmv.py -D 20070215-20070220 -t")
    #else: parser.print_help()
