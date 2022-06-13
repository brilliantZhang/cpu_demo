import streamlit as st


import pandas as pd
import numpy as np
import pickle
import xgboost as xgb

from clickhouse_driver import Client

machid=st.experimental_get_query_params()['machid'][0]
client = Client(host='localhost', settings={'use_numpy': True})
data=client.query_dataframe("""SELECT * FROM  default.mach M left join default.cpu C on M.cpuid=
C.cpuid""")

#C.cpuid where M.machid={}""".format(machid))

data.drop("C_cpuid",axis=1,inplace=True)
data.columns=['machid', 'Vendor', 'System', 'Cores', 'Chips', 'Memory',
       'MemoSize(GB)', 'MemoNum', 'Storage', 'DiskSize(TB)', 'SSD',
       'OperatingSystem', 'OS', 'OSVersion', 'KernelVersion', 'FileSystem',
       'Compiler', 'FC', 'FV', 'CC', 'CV', 'C++C', 'C++V', 'cpuid', 'cpu',
       'Processor', 'ProcessorMHz', 'CPU(s)Orderable', 'Parallel',
       'BasePointerSize', 'PeakPointerSize', 'CoresPerCPU', 'L1Dcache(KB)',
       'L1Icache(KB)', 'L2cache(MB)', 'L3cache(MB)', 'OtherCache']

test=data.iloc[[machid]]

cate_col=['Vendor',  'MemoNum', 
        'OS', 'KernelVersion', 'FileSystem','FC', 'FV', 'CC', 'CV', 'C++C', 'C++V',
           'Processor', 'Parallel','BasePointerSize', 'PeakPointerSize','OtherCache']
feat_cols=['Vendor', 'Cores', 'Chips', 'MemoSize(GB)', 'MemoNum', 'DiskSize(TB)', 
        'OS', 'KernelVersion', 'FileSystem','FC', 'FV', 'CC', 'CV', 'C++C', 'C++V',
           'Processor', 'ProcessorMHz', 'Parallel',
       'BasePointerSize', 'PeakPointerSize', 'CoresPerCPU', 'L1Dcache(KB)',
       'L1Icache(KB)', 'L2cache(MB)', 'L3cache(MB)', 'OtherCache']
numeric_col = ['Cores', 'Chips', 'MemoSize(GB)',  'DiskSize(TB)', 
         'ProcessorMHz',  'CoresPerCPU', 'L1Dcache(KB)',
       'L1Icache(KB)', 'L2cache(MB)', 'L3cache(MB)']   

st.markdown("# 跑分预测 ❄️")
st.sidebar.markdown("# prediction ❄️")

for col in numeric_col:
    test[col]=test[col].astype(float)
    number = st.number_input('修改参数 {}'.format(col),value=test[col].values[0],min_value=0.0,max_value=10000.0,step=1.0)
    st.write('原始值为{}，当前设置大小{} '.format(test[col].values[0],number))
    test[col]=number

for col in cate_col:
    option = st.selectbox('修改参数 {}'.format(col),data[col].unique())

    st.write('原始值为：{}，你修改为:{}'.format(test[col].values[0],option))
    test[col]=option

model = pickle.load(open("save_model/xgboost.pkl", "rb"))
tmp=test.copy()
if st.button('预测跑分'):
    
    for i,col in enumerate(cate_col):
        with open('save_model/{}_encoder.pkl'.format(i+1),'rb') as f:
            le = pickle.load(f)
    
        test[col]=le.transform(test[col])

    for benchmark in ['CINT2017', 'CINT2017rate', 'CFP2017rate', 'CFP2017']:
        test.loc[:,'Benchmark']=benchmark
        #tset=test.insert(test.shape[1], 'Benchmark', benchmark)
        print(test.columns)
        with open('save_model/0_encoder.pkl','rb') as f:
            le = pickle.load(f)

        test['Benchmark']=le.transform(test['Benchmark'])


        dtest=xgb.DMatrix(test[['Benchmark']+feat_cols],feature_names=['Benchmark']+feat_cols)

        st.write('{}预测跑分：{}'.format(benchmark,np.exp(model.predict(dtest))[0]))
    test=tmp
else:
    st.write('等待预测')









    