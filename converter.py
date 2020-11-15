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


'''
can set wieght for each label
set low high standard for label
return numpy array
'''
def label_converter(label, col, W, lowhigh):
    low, high = lowhigh
    
    def encode(v):
        if v<low:
            return 0
        elif v>high:
            return 2
        else:
            return 1
    
    # fill na 
    label.fillna(3)
    label[col] = label['agg_ex_'+col]*W[0] + label['self_'+col]*W[1] + label['partner_'+col]*W[2]
    label[col] = round(label[col]/sum(W))
    label[col] = label[col].apply(encode)
    label = label[['seconds',col]]
    np_label = np.array(label[[col]])
    l = len(np_label)
    return np_label.astype('int8').reshape(l,)



'''
window 
moving one by one
use thershold for labeling
'''
def label_window(yArr, win, th):
    arr = []
    l = len(yArr)
    for i in range(l-win+1):
        y = yArr[i:i+win]
        if np.count_nonzero(y == 2) >= th:
            arr.append(2)
        elif np.count_nonzero(y == 0) >= th:
            arr.append(0)
        else:
            arr.append(1)
            
    return np.array(arr)


def data_window(xArr, win):
    l = len(xArr)
    arr = []
    win = 6
    for i in range(l-win+1):
        x = xArr[i:i+win]
        x = np.concatenate(x)
        arr.append(x)
    
    return np.array(arr)