#!/usr/bin/env python
import spacy
from gensim.models import KeyedVectors
import os, traceback, re
import logging as log
from nltk.tokenize import word_tokenize
import json
__author__ = "Arturas"
__version__ = "1.0.1"
__description__ = """
@input: text of the headline.
@return returns Article object initialized from the headline.
"""

log.getLogger("gensim").setLevel(log.WARN)
log.getLogger().setLevel(log.WARN)

class Article:
    def __init__(self,headline,opts=None):
        log.debug('init Article')
        self._headline = headline
        self.algo = 'dflt' 
        self.query = 'dflt@{"subj":[],"verb":[],"obj":[]}'
        self.svo = {"subj":[],"verb":[],"obj":[]}
        self.querySVO = {"subj":[],"verb":[],"obj":[],"literals":[],"neg":[]}
        self.literals = []
        self.negatives = []
        self.keywords = []
        self.topic = None
        self.opts = opts
        self.nlp = spacy.load('en')
        if self.opts and hasattr(self.opts, 'dataDir') and self.opts.dataDir: self.dataDir = self.opts.dataDir
        else: self.dataDir = 'data'
        self.datafile = self.dataDir + '/GoogleNews-vectors-negative300.bin'
        # read file with special words
        self.tag_path = self.dataDir + '/tags.csv'
        self.tags_w = []
        self.tag_map = {}
        with open(self.tag_path) as fh1:
            lns1 = [ln1.rstrip('\n').split(',') for ln1 in fh1]
            self.tags_w = [ln1[0] for ln1 in lns1]
            self.tag_map = {ln1[0]:ln1[-1].split('|') if ln1[-1] else [] for ln1 in lns1}
        if self._headline and len(self._headline)>1:
            self.stop_words = ['report']
            for swd in self.stop_words:
                spacy.lang.en.stop_words.STOP_WORDS.add(swd)
                if swd in self.nlp.vocab: self.nlp.vocab[swd].is_stop = True
            log.debug('loaded spacy en model')
            self.newspath = self.dataDir + '/2007'
            self.model = KeyedVectors.load_word2vec_format(self.datafile, binary=True, limit=50000)
            #self.model = KeyedVectors.load_word2vec_format(self.datafile, binary=True)
            log.debug('initialized word2vec')
            #tst1 = self.model.most_similar(positive=['all'], topn=1) # array of tuples:[('wd1',prob1),('wd2',prob2)..]
        self.init()

    def init(self):
        hdr_words = word_tokenize(self._headline.lower())
        if len(hdr_words)>2:
            self.algo = 'any1st2'
            self.findSimilar()
        else:
            self.algo = 'allsim'
            self.svo = {"subj":[],"verb":[],"obj":[]}
            self.literals = hdr_words
        self.setKeywords()

    def hdr2str(self, hdrs):
        if not hdrs or len(hdrs)==0:
            log.error('cannot convert empty hdrs')
            return "[[]]"
        else:
            return "[["+'],['.join(["'"+"','".join(sorted([ee for ee in rr if ee is not None]))+"'" for rr in hdrs if len(rr)>0])+"]]"

    def getSVO(self, text=None):
        try:
            from soe import findSVOs
        except:
            from sys import path
            path.append('..')
            from p.soe import findSVOs
        if not text: text = self._headline
        for le in self.literals:
            text = text.replace(le,'')
        self.doc = self.nlp(str(text))
        if log.DEBUG>=log.getLogger().getEffectiveLevel():print('entities:',[(x.text, x.label_) for x in self.doc.ents])
        svos = findSVOs(self.doc)
        if log.DEBUG>=log.getLogger().getEffectiveLevel(): log.warn('svos: %s',svos)
        return svos # returns an array of subarrays, each subarray has 3 elements: [[s1,v1,o1],[s1,v1,o1]..]

    def getSVOKWords(self, text=None):
        svo = self.getSVO(text)
        hdr_words = word_tokenize(text.lower())
        tags_w = [wd1 for wd1 in list(set(hdr_words) & set(self.tags_w))]
        if tags_w: svo.append(tags_w)
        return svo

    def getTags(self, text=None):
        if not text: text = self._headline
        hdr_words = word_tokenize(text.lower())
        tags_w = [wd1 for wd1 in list(set(hdr_words) & set(self.tags_w))]
        tags = []
        #tags = [tt for tt in self.tag_map[tag] for tag in tags_w if tag in self.tag_map]
        for tag in tags_w:
            if tag in self.tag_map:
                tags.extend(self.tag_map[tag])
        return tags

    def findSimilar(self):
        # extract subject-verb-object
        svos = self.getSVO() # array of subarrays of len 3
        # if cannot find any keywords look for entire string
        if not svos or len(svos)<=0 or len(svos[0])<=2: #return []
            if len(self.literals) == 0: self.literals = [self._headline]
            return []
        svo = {"subj":[],"verb":[],"obj":[]}
        if self.opts and self.opts.entity: # if entity specified
            for ee in self.doc.ents:
                if ee.label_=='ORG' or ee.label_=='PERSON':
                    svo['subj'].append(ee.text);svo['verb'].append(ee.text);svo['obj'].append(ee.text)
        hmap = {0:'subj',1:'verb',2:'obj'}
        for ii in range(3):
            if not svos[0][ii] in self.nlp.Defaults.stop_words and svos[0][ii]:
                svo[hmap[ii]].append(svos[0][ii])
            in_wds = []
            for tpl in svos:
                if tpl[ii] in self.model.vocab: in_wds.append(tpl[ii])
            if len(in_wds)<=0: continue
            simwds = self.model.most_similar(positive=in_wds, topn=2) #array of tuples:[('wd1',prob1),('wd2',prob2)..]
            if not svo[hmap[ii]]: svo[hmap[ii]] = [in_wds[0]]
            elif not in_wds[0] in svo[hmap[ii]]: svo[hmap[ii]].append(in_wds[0])
            svo[hmap[ii]].extend([ee[0].lower() for ee in simwds if ee[0].lower() not in svo[hmap[ii]]])
        self.svo = svo.copy()
        self.querySVO = svo.copy()

    def setKeywords(self, keywordsStr=""):
        if len(keywordsStr)>3:
            self.query = keywordsStr
            qquery,qkeywords = keywordsStr.split('@')
            if len(qquery): self.algo = qquery
            if len(qkeywords)>0: self.querySVO = eval(qkeywords)
        if "negatives" in self.querySVO: self.querySVO["neg"]=self.querySVO["negatives"]
        if len(self.svo)>0: self.setSVOFromQuerySVORep()
        self.keywords.extend(self.svo["subj"]);self.keywords.extend(self.svo["verb"]);self.keywords.extend(self.svo["obj"])
        if self.literals: self.keywords.extend(self.literals)
        if self.negatives: self.keywords.extend(self.negatives)
        self.keywords = list(set(self.keywords)) # remove duplicates

    def getKeywords(self):
        return self.algo+'@'+json.dumps(self.querySVO)

    def setSVOFromQuerySVORep(self):
        hdr_words = word_tokenize(self._headline.lower())
        # set any special words and stop words from the headline
        x_words = [wd1 for wd1 in list(set(hdr_words) & set(self.tags_w))]
        # extend literals with special words
        self.literals.extend(x_words)
        #for x_w in x_words: 
        #    if re.search(x_w.lower(), self._headline.lower()): # do we need this line?
        #        self.literals.append(x_w)
        self.svo = {"subj":self.querySVO["subj"],"verb":self.querySVO["verb"],"obj":self.querySVO["obj"]}
        if "literals" in self.querySVO: self.literals.extend(self.querySVO["literals"])
        if "neg" in self.querySVO: self.negatives.extend(self.querySVO["neg"])
        self.literals = list(set(self.literals))
        self.negatives= list(set(self.negatives))
        self.querySVO = {"subj":self.svo["subj"],"verb":self.svo["verb"],"obj":self.svo["obj"]}
        if self.literals: self.querySVO["literals"] = self.literals
        if self.negatives: self.querySVO["neg"] = self.negatives

    def setQueryFunc(self, qquery):
        self.algo = qquery
