# This folder contains newsdai framework code and scripts

## Overview
NEWSDAI: TRADER SEARCH ENGINE AND PREDICTIVE ANALYTICS TOOLKIT
Given market moves, finds related news and relevant concepts. Given a concept, finds past market moves and so on.

Problem statement: in a fast pace trading environment where every millisecond counts
crunching through gigabytes of unstructured documentation in second gives you a distinct advantage over your competition.

## Installation instructions

### Solr Installation
* download solr: https://lucene.apache.org/solr/downloads.html
* tar zxf solr-8.2.0.tgz
* ~/bin/solr start -Dsolr.clustering.enabled=true

### KDB/q Installation
* download q executable: https://kx.com/connect-with-us/download/
* for linux install and unzip l64.zip
* run q/l64/q

### Newsdai Installation
* clone repository
* `cd newsdai`
* install python3.7 (on mac brew install python, on windows https://www.python.org/downloads/windows/)
* create py3.6 environment: `conda create --name py37 python=3.7`
* `conda activate py36`
* download and install kdb/q distribution with embedPy into py36 environment
* `pip install -r docs/requirements.txt`
* pip install qpython
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

```
conda activate py36
> solr start -Dsolr.clustering.enabled=true # run solr
> cd ~/$newsdaiPath/data/solr; python json2solrIndx.py -l # run to import data into solr
> cd hdb; q newsdai
q) .Q.chk[`:.]
cd -
bokeh serve newsdai_mktmv
```

