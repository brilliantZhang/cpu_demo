import streamlit as st
import pandas as pd
import numpy as np
st.markdown("# 服务器详细信息 🎉")
st.sidebar.markdown("# details 🎉")


from clickhouse_driver import Client

machid=st.experimental_get_query_params()['machid'][0]
client = Client(host='localhost', settings={'use_numpy': True})
data=client.query_dataframe("""SELECT * FROM  default.mach M left join default.cpu C on M.cpuid=
C.cpuid where M.machid={}""".format(machid))
cols=['machid', 'Vendor', 'System', 'Cores', 'Chips', 'Memory',
       'MemoSize(GB)', 'MemoNum', 'Storage', 'DiskSize(TB)', 'SSD',
       'OperatingSystem', 'OS', 'OSVersion', 'KernelVersion', 'FileSystem',
       'Compiler', 'FC', 'FV', 'CC', 'CV', 'C++C', 'C++V', 'cpuid', 'cpu',
       'Processor', 'ProcessorMHz', 'CPU(s)Orderable', 'Parallel',
       'BasePointerSize', 'PeakPointerSize', 'CoresPerCPU', 'L1Dcache(KB)',
       'L1Icache(KB)', 'L2cache(MB)', 'L3cache(MB)', 'OtherCache']
data.drop("C_cpuid",axis=1,inplace=True)
data.columns=cols

st.markdown("# details page 🎈")

st.markdown("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
<style type="text/css" id="internalScreen" media="screen">
<!--
  @import url(//www.spec.org/includes/css/cpu2017result.css);
  @import url(//www.spec.org/includes/css/cpu2017screen.css);
-->
  @import url(//https://cdn.staticfile.org/antd/3.23.6/antd.css);
</style>
<style type="text/css" id="internalPrint" media="print">
<!--
  @import url(//www.spec.org/includes/css/cpu2017result.css);
  @import url(//www.spec.org/includes/css/cpu2017print.css);
-->
  @import url(//https://cdn.staticfile.org/antd/3.23.6/antd.css);
</style>
</head>
<body>
<div class="resultpage" >
 <div class="titlebarcontainer">
  <div class="logo">
   <a href="/" style="border: none"><img src="http://www.spec.org/cpu2017/results/images/logo037.gif" alt="SPEC Seal of Reviewal" /></a>
  </div>
  <div class="titlebar">
   <p style="font-size: smaller">Copyright 2017-2022 Standard Performance Evaluation Corporation</p>
  </div>
 </div>
 <table class="titlebarcontainer">
  <tr>
   <td class="systembar" rowspan="2">
    <p>{} </p>
    <p>{}<br />
({}, {})<br />    </p>
   </td>
   </table>
 <div class="infobox" style='display:flex'>
 	<div style=' float: left;'>
  <table id="Hardware" style='width:100%'
         summary="Details of hardware configuration for the system under test">
   <thead>
    <tr><th colspan="2"><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Hardware">Hardware</a></th></tr>
   </thead>
   <tbody>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#CPUName">CPU Name</a>:</th>
     <td>{}</td>
    </tr>    
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Nominal">Nominal</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Enabled">Enabled</a>:</th>
     <td>{} cores, {} chips</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Orderable">Orderable</a>:</th>
     <td>{} chip(s)</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#CacheL1">Cache L1</a>:</th>
     <td>{} KB I + {} KB D on chip per core</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#L2">L2</a>:</th>
     <td>{} MB I+D on chip per core</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#L3">L3</a>:</th>
     <td>{} MB I+D on chip per chip</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Other">Other</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Memory">Memory</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Storage">Storage</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Other">Other</a>:</th>
     <td>None</td>
    </tr>
   </tbody>
  </table>
</div>
<div  style=' float: right;'>
  <table id="Software" style='width:100%; float: right;'
         summary="Details of software configuration for the system under test">
   <thead>
    <tr><th colspan="2"><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Software">Software</a></th></tr>
   </thead>
   <tbody>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#OS">OS</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">FortranCompiler</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">FortranCompilerVersion</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">C Compiler</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">C CompilerVersion</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">C++ Compiler</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">C++ CompilerVersion</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Parallel">Parallel</a>:</th>
     <td>{}</td>
    </tr>    
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#FileSystem">File System</a>:</th>
     <td>{}</td>
    </tr>    
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#BasePointers">Base Pointers</a>:</th>
     <td>{}</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#PeakPointers">Peak Pointers</a>:</th>
     <td>{}</td>
    </tr>   
    
   </tbody>
  </table>
</div>
</div>
</body>
</html>""".format(data['Vendor'].values[0], data['System'].values[0], data['ProcessorMHz'].values[0], 
data['Processor'].values[0],
 data['Processor'].values[0]
, data['ProcessorMHz'].values[0], data['Cores'].values[0], data['Chips'].values[0], data['CPU(s)Orderable'].values[0], 
data['L1Icache(KB)'].values[0],
data['L1Dcache(KB)'].values[0],data['L2cache(MB)'].values[0],  data['L3cache(MB)'].values[0], data['OtherCache'].values[0],
  data['Memory'].values[0], 
data[ 'Storage'].values[0], data['OperatingSystem'].values[0], data['FC'].values[0], data['FV'].values[0],  
data['CC'].values[0], data['CV'].values[0], data['C++C'].values[0],
 data['C++V'].values[0],  data['Parallel'].values[0], data['FileSystem'].values[0], 
 data['BasePointerSize'].values[0],  data['PeakPointerSize'].values[0]),unsafe_allow_html=True)


se_col=['System', 'Cores', 'Chips', 'Memory',
       'MemoSize_GB_', 'MemoNum', 'Storage', 'DiskSize_TB_',
       'OperatingSystem',  'KernelVersion', 'FileSystem','Compiler', 'cpu']

#同CPU其他机子
data=client.query_dataframe("""SELECT * FROM  default.mach M1 ,(SELECT cpu FROM  default.mach t  where t.machid={}) 
M2 where M1.cpu=M2.cpu""".format(machid))
idx=data[['System']].drop_duplicates()[:10].index
st.write('😎根据你所查看的服务器的CPU型号，为你推荐其他你可能感兴趣的服务器：')
st.write('关键参数对比，最大值黄色高亮\n')
st.dataframe(data.loc[idx,se_col].style.highlight_max(subset=['Cores', 'Chips', 'MemoSize_GB_',  'DiskSize_TB_']))

#同厂商其他机子
data=client.query_dataframe("""SELECT * FROM  default.mach M1 ,(SELECT Vendor FROM  default.mach t  where t.machid={}) 
M2 where M1.Vendor=M2.Vendor""".format(machid))
idx=data[['System']].drop_duplicates()[:10].index
st.write('😎根据你所查看的服务器的生产厂商，为你推荐其他你可能感兴趣的服务器：')
st.write('关键参数对比，最大值黄色高亮\n')
st.dataframe(data.loc[idx,se_col].style.highlight_max(subset=['Cores', 'Chips', 'MemoSize_GB_',  'DiskSize_TB_']))

#同tag其他机子
tags=client.query_dataframe("""select * from  tags T ,(SELECT * FROM  mach  where mach.machid={}) 
M where T.`L.C.Processor`=M.cpu """.format(machid))
tag_col=[x for x in tags.columns if '_tag' in x]
has_tag=[]
for tag in tag_col:
  if tags[tag].values[0]>0:
    has_tag.append(tag)
if len(has_tag)>0:
  st.write('你所查看的服务器适用领域标签为：',has_tag)
  data=client.query_dataframe("""select * from  tags T ,(SELECT * FROM  mach) 
  M where T.`L.C.Processor`=M.cpu and T.{}=1""".format(has_tag[0]))

  idx=data[['System']].drop_duplicates()[:10].index
  st.write('😎根据你所查看的服务器的适用领域标签，为你推荐其他你可能感兴趣的服务器：')
  st.write('关键参数对比，最大值黄色高亮\n')
  st.dataframe(data.loc[idx,se_col].style.highlight_max(subset=['Cores', 'Chips', 'MemoSize_GB_',  'DiskSize_TB_']))