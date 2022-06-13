import streamlit as st

st.markdown("# Main page 😎")
st.sidebar.markdown("# Main page 😎")

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
    <p>ASUSTeK Computer Inc.    </p>
    <p>ASUS RS700-E10(Z12PP-D32) Server System<br />
(2.10 GHz, Intel Xeon Gold 5318Y)<br />    </p>
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
     <td>Intel Xeon Gold 5318Y</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#MaxMHz">Max MHz</a>:</th>
     <td>3400</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Nominal">Nominal</a>:</th>
     <td>2100</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Enabled">Enabled</a>:</th>
     <td>48 cores, 2 chips, 2 threads/core</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Orderable">Orderable</a>:</th>
     <td>1, 2 chip(s)</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#CacheL1">Cache L1</a>:</th>
     <td>32 KB I + 48 KB D on chip per core</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#L2">L2</a>:</th>
     <td>1.25 MB I+D on chip per core</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#L3">L3</a>:</th>
     <td>36 MB I+D on chip per chip</td>
    </tr>
    <tr>
     <th>&nbsp;&nbsp;<a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Other">Other</a>:</th>
     <td>None</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Memory">Memory</a>:</th>
     <td>1 TB (16 x 64 GB 2Rx4 PC4-3200AA-R,<br />
running at 2933)<br /></td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Storage">Storage</a>:</th>
     <td>1 x 4 TB PCIE NVME SSD</td>
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
     <td>Red Hat Enterprise Linux release 8.3 (Ootpa)<br />
4.18.0-240.22.1.el8_3.x86_64<br /></td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Compiler">Compiler</a>:</th>
     <td>C/C++: Version 2021.1 of Intel oneAPI DPC++/C++<br />
Compiler Build 20201113 for Linux;<br />
Fortran: Version 2021.1 of Intel Fortran Compiler<br />
Classic Build 20201112 for Linux;<br />
C/C++: Version 2021.1 of Intel C/C++ Compiler<br />
Classic Build 20201112 for Linux<br /></td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Parallel">Parallel</a>:</th>
     <td>No</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Firmware">Firmware</a>:</th>
     <td>Version 0504 released May-2021</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#FileSystem">File System</a>:</th>
     <td>xfs</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#SystemState">System State</a>:</th>
     <td>Run level 3 (multi-user)</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#BasePointers">Base Pointers</a>:</th>
     <td>64-bit</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#PeakPointers">Peak Pointers</a>:</th>
     <td>64-bit</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#Other">Other</a>:</th>
     <td>jemalloc memory allocator V5.0.1</td>
    </tr>
    <tr>
     <th><a href="http://www.spec.org/auto/cpu2017/Docs/result-fields.html#PowerManagement">Power Management</a>:</th>
     <td>BIOS and OS set to prefer performance<br />
at the cost of additional power usage.<br /></td>
    </tr>
   </tbody>
  </table>
</div>
</div>
</body>
</html>""",unsafe_allow_html=True)