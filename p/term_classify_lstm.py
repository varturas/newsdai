#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
import sys, os, traceback
from argparse import ArgumentParser
import logging as log
import pandas as pd
import numpy as np
import keras
from keras.preprocessing import sequence
from keras.models import Sequential
from keras.layers import Dense,LSTM
from keras.layers import Flatten
#from keras.layers import Embedding,TimeDistributed,AveragePooling1D
#from keras.layers import Conv1D, MaxPooling1D
from keras.datasets import imdb

os.environ['KMP_DUPLICATE_LIB_OK']='True'

log.getLogger().setLevel(log.WARN)

class TFA:
    dataPath = "../data"
    tfFile = dataPath + "/termFreq"
    tfFileCsv = tfFile + ".csv"
    tfFilePkl = tfFile + ".pkl"

    def __init__(self, args):
        self.args = args
        self.file = self.args.file
        self._df = pd.read_pickle(self.file)
        self.init_model()

    def init_model(self):
        # Convolution
        kernel_size = 5
        filters = 64
        pool_size = 4
        # LSTM
        self.seq_len = seq_len = 158
        lstm_output_size = 4
        n_rows = 2000
        # Training
        self.batch_size = batch_size = 128
        self.epochs = epochs = 2
        self.model = Sequential()
        #self.model.add(LSTM(batch_size, input_shape=(None, 1), dropout=0.2, recurrent_dropout=0.2, return_sequences=True))
        self.model.add(LSTM(32, input_shape=(seq_len, 1), return_sequences=True))
        self.model.add(LSTM(32, return_sequences=True))
        #self.model.add(TimeDistributed(Dense(1)))
        #self.model.add(AveragePooling1D())
        self.model.add(Flatten())
        self.model.add(Dense(lstm_output_size, activation='softmax'))
        self.model.compile(loss='categorical_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])
        return self.model

    def load_data(self):
        x_train, y_train, x_test, y_test = [], [], [], []
        self.legend = { 0:'risk', 1:'signal', 2:'cycle', 3:'noise'}
        self.trainTags = {0:['turmoil','crunch','subprim','turbul','writedown','slowdown'], \
            1:['restrict','viper','aftermath','slash','victim','dunn'], \
            2:['deposit','reclassifi','occup','exclud','loan','substitut','sequenti','net','payabl','bear','royalti','januari'], \
            3:['rumour','hooker','equival','truce','prima','synerget','sceptic','stemcel','compens','monogram','brawl'] \
            }
        self.testTags = {0:['write-down','recess','crisi'], \
            1:['farnborough','aa-','devast','damag'], \
            2:['interest','charg','incur','taxat','semicon','non-recur','three-month','account','non-cas'], \
            3:['comtempt','smallpox','pullout','fuelcell'] \
            }
        for kk,tags in self.trainTags.items():
            for tag in tags:
                if not np.any(self._df.tag==tag): continue
                try:
                    vv = self._df.loc[self._df.tag==tag,['tdf']].values.flatten()[0].tolist()
                    nv = [float(ee)/sum(vv) for ee in vv]
                    x_train.append(nv)
                    #x_train.append(vv)
                    nk = [0]*len(self.trainTags.keys())
                    nk[kk]=1
                    y_train.append(nk)
                except:
                    log.warn('error processing a tag:{}'.format(tag))
                    traceback.print_exc()
        for kk,tags in self.testTags.items():
            for tag in tags:
                if not np.any(self._df.tag==tag): continue
                try:
                    vv = self._df.loc[self._df.tag==tag,['tdf']].values.flatten()[0].tolist()
                    nv = [float(ee)/sum(vv) for ee in vv]
                    x_test.append(nv)
                    #x_test.append(vv)
                    nk = [0]*len(self.testTags.keys())
                    nk[kk]=1
                    y_test.append(nk)
                except:
                    log.warn('error processing a tag:{}'.format(tag))
                    traceback.print_exc()
        x_train = sequence.pad_sequences(x_train, maxlen=self.seq_len, dtype='float32')
        x_test = sequence.pad_sequences(x_test, maxlen=self.seq_len, dtype='float32')
        x_train = np.array([np.array(ee).reshape(self.seq_len,1) for ee in x_train])
        y_train = np.array(y_train)
        x_test = np.array([np.array(ee).reshape(self.seq_len,1) for ee in x_test])
        y_test = np.array(y_test)
        return (x_train,y_train),(x_test,y_test)

    def train_model(self, x_train, y_train, x_test, y_test):
        self.model.fit(x_train, y_train,
            batch_size=self.batch_size,
            epochs=self.epochs,
            validation_data=(x_test, y_test))

    def evaluate(self, x_test, y_test):
        score = self.model.evaluate(x_test, y_test,
                       batch_size=self.batch_size, verbose=1)
        log.warn('Test score: {}'.format(score[0]))
        log.warn('Test accuracy: {}'.format(score[1]))

    def train(self):
        self.get_model()

    def predict(self):
        self.get_model()
        xx,tt = [],[]
        for ix,row in self._df.iterrows():
            vv = row['tdf'].tolist()
            if sum(vv) == 0: continue
            tag = row['tag']
            nv = [float(ee)/sum(vv) for ee in vv]
            xx.append(nv)
            tt.append(tag)
        x_data = sequence.pad_sequences(xx, maxlen=self.seq_len, dtype='float32')
        x_data = np.array([np.array(ee).reshape(self.seq_len,1) for ee in x_data])
        yhat = self.model.predict(x_data)
        y_cls = yhat.argmax(axis=-1)
        for ii in range(len(x_data)):
            print("tag={}, prediction={}".format(tt[ii], self.legend[y_cls[ii]]))
        #print(yhat)

    def get_model(self):
        modelPath = self.dataPath + "/termModel"
        (x_train, y_train), (x_test, y_test) = self.load_data()
        if not self.args.force and os.path.exists(modelPath): 
            self.model = keras.models.load_model(modelPath)
        else: 
            log.warn('train sequences len {}'.format(len(x_train)))
            log.warn('x_train shape:{}'.format(x_train.shape))
            self.train_model(x_train, y_train, x_test, y_test)
            self.model.save(modelPath)
        self.evaluate(x_test, y_test)
        return self.model

def main(args):
    if hasattr(args,'train') and args.train:
        if not args.file: args.file=TFA.tfFilePkl 
        tfObj = TFA(args)
        tfObj.train()
    elif hasattr(args,'predict') and args.predict:
        if not args.file: args.file=TFA.tfFilePkl 
        tfObj = TFA(args)
        tfObj.predict()
    else: parser.print_help()

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-F', action='store', dest='file', nargs='?', default=None, const=TFA.tfFilePkl, help='input term freq file')
    parser.add_argument('-f', action='store_true', dest='force', help='force recalculation')
    parser.add_argument('-t', action='store_true', dest='train', help='train model on the terms')
    parser.add_argument('-p', action='store_true', dest='predict', help='predict using trained model')
    args = parser.parse_args()
    main(args)
    sys.exit(0)

