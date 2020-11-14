import os
import pandas as pd
import numpy as np
from os.path import *


'''
Load csv and convert to X, y numpy 
user by user 
'''


'''
cut data df 
change timestamp to sec
'''
def match_ts(start, end, df, cols):
    #drop columns
    cols.append('timestamp')

    df = df[cols]
    #cut df timestamp
    df = df.query('timestamp >= ' + str(start)).query('timestamp <= '+ str(end))
    # reset timestamp
    df['timestamp'] = (df['timestamp']-start)//1000
    # window by mean
    df = df.groupby('timestamp').mean()
    # fill missing with mean
    df = df.fillna(df.mean())

    num = len(df)//5
    np_arr = np.array(df)
    d = len(cols)-1
    n = num*5

    return np_arr[:n].reshape(num,5,d)


def onehot_label(label, col):
    label['arousal'] = label['agg_ex_arousal'] + label['self_arousal'] + label['partner_arousal']
    label['valence'] = label['agg_ex_valence'] + label['self_valence'] + label['partner_valence']
    label['arousal'] = round(label['arousal'] / 3)
    label['valence'] = round(label['valence'] / 3)
    label = label[['seconds',col]]
    label['l'] = label[col]<3.0
    label['m'] = label[col]==3.0
    label['h'] = label[col]>3.0
    np_label = np.array(label[['l','m','h']])
    return np_label.astype('int32')
