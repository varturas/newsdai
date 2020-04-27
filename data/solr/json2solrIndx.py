#!/usr/bin/env python
import sys,os,traceback
from lib.Article import Article
import solr,json

dataDir = "../newsJson"
#solrUrl = 'solr.newsdai.com'
solrUrl = 'localhost:8983'

def main(args):
    s = solr.SolrConnection('http://'+solrUrl+'/solr/newsdai')
    json_files = [pos_json for pos_json in os.listdir(dataDir) if pos_json.endswith('.json')]
    icnt=0
    class Options:
        dataDir = '..'
    opts = Options()
    a = Article("",opts)
    for ii, js in enumerate(json_files):
        if ii%21==0: print('processing {}'.format(js))
        with open(os.path.join(dataDir, js)) as json_file:
            json_txt = json.load(json_file)
            for jj,doc in enumerate(json_txt):
#{ "id":icnt, "Headline":ee['Headline'], #"CompanyCodes":ee['CompanyCodes'], "GmtTimeStamp":ee['GmtTimeStamp'] }
                try:
                    svo = a.getSVO(doc['Headline'])
                    if svo and len(svo)>1:
                        doc['SVO'] = svo[0]
                    s.add(**doc, commit=True)
                except:
                    traceback.print_exc()
                if icnt%1001==0: print('adding {} doc'.format(icnt),end='\r',flush=True) 
                icnt+=1

if __name__ == '__main__':
    main(sys.argv)
