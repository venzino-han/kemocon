import os
import pandas as pd
import numpy as np
from os.path import *

'''
Load data from folders
'''

ROOT = 'data/raw/'
NEURO = ROOT+'neuro'
E4 = ROOT+'e4'
ANNO = ROOT+'anno'

neuro_cols = ['timestamp', 'delta', 'lowAlpha', 'lowBeta', 'highAlpha','highBeta','lowGamma', 'middleGamma', 'theta', 'value']
e4_cols = ['timestamp', 'x', 'y', 'z', 'value']
label_cols = ['seconds','arousal','valence']

# GET All file in dir
def files_in_dir(root_dir):
    fileList = []
    files = os.listdir(root_dir)
    for file in files:
        path = os.path.join(root_dir, file)
        if isfile(path):
            fileList.append(path)
        if os.path.isdir(path):
            fileList += files_in_dir(path)

    fileList = [x for x in fileList if ('checkpoint' not in x)]

    return fileList

def get_data_files(uid, dataType):
    file_pathss = files_in_dir(ROOT+dataType+'/'+str(uid))
    dfs = {}
    for path in file_pathss:
        k = path.split('\\')[-1].split('.')[0]
        dfs[k] = pd.read_csv(path)

    return dfs



# get lable file
def get_label_files(uid):
    anno_types = ['self', 'partner', 'external', 'agg_ex']
    anno_file_dic = {}

    for t in anno_types:
        u_anno_path = os.path.join(ANNO, t)
        anno_data = files_in_dir(u_anno_path)
        anno_file_dic[t] = anno_data

    u_dic = {}
    for t in anno_types:
        files = anno_file_dic[t]
        for f in files:
            f_ = f.split('\\')[-1]
            p = f_.split('.')[0]
            if p == 'P' + uid and 'checkpoint' not in f_:
                if t in u_dic:
                    u_dic[t].append(f)
                else:
                    u_dic[t] = [f]
    return u_dic
#
#Get label data from dict
def get_label_data(label_files, agg_only=True):
    dfs={}
    for k,v in label_files.items():
        if agg_only and k == 'external':
            continue
        dfs[k] = [pd.read_csv(x) for x in v]

    return dfs

from collections import defaultdict
# Concate all dfs
# change duplicate col name
# dfs : Dict
def concat_all(dfs, result_cols, concat_at = 'timestamp'):
    #build col dict
    col_dict = defaultdict(int)
    for df in dfs.values():
        if type(df)== list :
            df = df[0]
        for col in list(df.columns):
            col_dict[col]+=1

    #change column name
    for t, df in dfs.items():
        if type(df)==list:
            df = df[0]
        for col in df.columns:
            if col not in result_cols:
                df.drop([col], axis = 1, inplace=True)
            elif col_dict[col] > 1 and col != concat_at:
                # print('rename column :' + col + '--> ' +t+'_'+col )
                df.rename(columns = {col: t+'_'+col}, inplace=True)

    #merge dfs
    df_list = [ x[0] if type(x) == list else  x  for x in dfs.values() ]

    df1 = df_list.pop()
    # print(df1)
    for df in df_list:
        df1 = pd.merge(df1, df, how='outer', on=concat_at)

    #fill every missing col zero
    for col in result_cols:
        if col not in df1.columns:
            df1[col] = 0

    return df1

def dataTransition(uid):
    uid = str(uid)

    #load label
    lfiles = get_label_files(uid)
    label_dfs = get_label_data(lfiles)
    label = concat_all(label_dfs,label_cols, concat_at ='seconds')
    label.to_csv(uid+'_label.csv')

    try:
        e4Data= get_data_files(uid, 'e4')
        e4Data = concat_all(e4Data, e4_cols, 'timestamp')
        e4Data.to_csv(uid+'_e4.csv')
    except:
        print(uid + ': No e4 Data')

    try:
        neuroData = get_data_files(uid, 'neuro')
        neuroData = concat_all(neuroData, neuro_cols, 'timestamp')
        neuroData.to_csv(uid + '_neuro.csv')
    except:
        print(uid + ': No neuro Data')