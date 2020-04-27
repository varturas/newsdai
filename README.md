# This folder contains newsdai framework code and scripts

## Overview
NEWSDAI: TRADER SEARCH ENGINE AND PREDICTIVE ANALYTICS TOOLKIT
Given market moves, finds related news and relevant concepts. Given a concept, finds past market moves and so on.

Problem statement: in a fast pace trading environment where every millisecond counts
crunching through gigabytes of unstructured documentation in second gives you a distinct advantage over your competition.

## Installation instructions

* clone repository
* `cd newsdai`
* create py3.6 environment: `conda create --name py36 python=3.6`
* `source activate py36`
* download and install kdb/q distribution with embedPy into py36 environment
* `pip install -r docs/requirements.txt`
* make sure gensim,spacy and nltk libraries are installed
* make sure QHOME points to anaconda environment: otherwise run QHOME=~/anaconda*/envs/py36/q

## Directory structure
* newsdai/ -- root
    * newsdai_mktmv - bokeh folder with main.py
    * p/       -- newsdai.p embedpy sources for ML/AI
    * data/    -- contains hdb, pyarrow and solr 

## table structure
* eodR - eod returns
* news - news articles

## Bokeh server
Running bokeh:
bokeh serve newsdai_mktmv
or on a server
nohup bokeh serve --allow-websocket-origin=newsdai.com newsdai_mktmv --port 11762 1>&/tmp/newsdai.log&


# TO RUN newsdai mkt move search
=================================
...
conda activate py36
> cd ~/$newsdaiPath/data/newsJson; q createJsonIndexFile.q # create json files with ret
q) convert[`$":../2006"] // create data/newsJson/*.json files
> cd ~/$newsdaiPath/data/solr; python json2solrIndx.py -l # run to import data into solr
> solr start -Dsolr.clustering.enabled=true # run solr
> cd ~/$newsdaiPath/data/utils; q load_eod.q
q) load_all[DIRS] // run load.q to create hdb/eod
> cd ~/$newsdaiPath/data/utils; q utils.q
q) saveRet[] //to create eodR with ret/ret5
cd ~/$newsdaiPath/utils; python json2tags.py # to create data/newsTags/*.json files
> cd ~/$newsdaiPath/data/utils; q utils.q
q) saveTags[`:newsTags;`news] // to create news with tags
> cd hdb; q newsdai
q) .Q.chk[`:.]
cd -
bokeh serve newsdai_mktmv
