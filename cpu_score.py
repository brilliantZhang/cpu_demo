#!/usr/bin/env python
# coding: utf-8

# In[179]:


import pandas as pd
import pymysql
import xlrd
import numpy as np
import numba
#打开数据所在的工作簿，以及选择存有数据的工作表
def group_feature(df, key, target, aggs):   
        k=key[0]
        #aggs是个列表 ['max','min','mean','std']
        agg_dict = []
        for ag in aggs:
            ll =(f'{k}_{target}_{ag}',ag)
            # agg_dict[f'{target}_{ag}_{window}'] = ag
            agg_dict.append(ll)
        
        t = df.groupby(key)[target].agg(agg_dict).reset_index()
        return t
def load_data():
    df = pd.read_csv("added_scores_details.csv")
    #df=df[df.Scores_details.isnull()==False]



    #df['Scores_details']=df.Scores_details.apply(lambda x:x.split(';'))
    #df['Scores_details']=df['Scores_details'].apply(lambda x:dict(zip([i.split(':')[0] for i in x],[i.split(':')[1] for i in x])))

    """keys=[]
                for r in df.iterrows():
                    keys.extend(list(r[1]['Scores_details'].keys()))
                keys=set(keys)    
            
            
                # In[182]:
            
            
                for col in keys:
                    values=[]
                    for r in df.iterrows():
                        try:
                            values.append(r[1]['Scores_details'][col])
                        except:
                            values.append(np.nan)
                    df[col]=values"""




    #df['Compiler']=df['Compiler'].apply(lambda x:x.split(':')[0])
    #df=df.rename(columns={'Hardware Vendor\t':'Hardware Vendor'})

    df=df[['# Chips ', 'Processor ', 'Processor MHz',
    '1st Level Cache', '2nd Level Cache',
    '3rd Level Cache',  '548.exchange2_r',
   '648.exchange2_s', '607.cactuBSSN_s', '505.mcf_r', '508.namd_r',
   '627.cam4_s', '619.lbm_s', '631.deepsjeng_s', '554.roms_r',
   '526.blender_r', '520.omnetpp_r', '511.povray_r', '500.perlbench_r',
   '544.nab_r', '507.cactuBSSN_r', '523.xalancbmk_r', '549.fotonik3d_r',
   '510.parest_r', '603.bwaves_s', '649.fotonik3d_s', '557.xz_r',
   '527.cam4_r', '531.deepsjeng_r', '623.xalancbmk_s', '525.x264_r',
   '605.mcf_s', '644.nab_s', '519.lbm_r', '538.imagick_r', '641.leela_s',
   '628.pop2_s', '638.imagick_s', '541.leela_r', '625.x264_s', '602.gcc_s',
   '600.perlbench_s', '620.omnetpp_s', '621.wrf_s', '657.xz_s',
   '521.wrf_r', '502.gcc_r', '503.bwaves_r', '654.roms_s']]


    df.fillna('NAN',inplace=True)
    df.columns=[df.columns[i].replace('#','').replace(' ','') for i in range(df.columns.shape[0])]
    #df.drop('Disclosure',axis=1,inplace=True)

    df.replace('NAN',0,inplace=True)
    df.replace('NC',0,inplace=True)
    return df




def get_app_weight_result(df,app2score,app2weight):
    #e.g. app2weight={'Perl interpreter': 0.3,'GNU C compiler':0.7}
    
    try:
        drop=['Processor_'+x+'_mean' for x in list(app2weight.keys())]+['Processor_final_score_mean']
        df.drop(drop,axis=1,inplace=True)
    except:
        pass


    # In[187]:

    
    df['final_score']=0.0
    for app in app2weight:
        weight=app2weight[app]
        df[app]=0.0
        for c in app2score[app]:
            df[c]=df[c].apply(lambda x:float(x)*weight)

            #df[app]=df.apply(lambda x:sum([float(x[c])*weight for c in app2score[app]]) ,axis=1)
            df[app]+=df[c]/df['Chips']
        df['final_score']+=df[app]


    # In[188]:


    t =  group_feature(df,['Processor'],'final_score',['mean'])
    df = df.merge(t,on=['Processor'],how='left')
    for app in app2weight.keys():
        t =  group_feature(df,['Processor'],app,['mean'])
        df = df.merge(t,on=['Processor'],how='left')

    res=df[['Processor','Processor_final_score_mean','ProcessorMHz','1stLevelCache','2ndLevelCache']+['Processor_'+x+'_mean' for x in list(app2weight.keys())]].drop_duplicates().sort_values('Processor_final_score_mean',ascending=False).head(10)
    res=res.rename(columns={'Processor_final_score_mean':'Processor Final Score'})
    res=res.rename(columns=dict(zip(['Processor_'+x+'_mean' for x in list(app2weight.keys())],list(app2weight.keys()))))
    
    res=res[['Processor', 'ProcessorMHz', '1stLevelCache',
           '2ndLevelCache']+list(app2weight.keys())+['Processor Final Score']].reset_index(drop=True)
    res.index+=1
    return res
