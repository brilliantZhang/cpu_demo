import streamlit as st
from PIL import Image
import json
import pandas
import cpu_score
import lxml
image = Image.open('CPU.png')
import numpy as np

st.image(image)

chklist = [
    ["Perl interpreter", "", ""],
    ["GNU C compiler", "", ""],
    ["Route planning", "", ""],
    ["XML to HTML conversion via XSLT", "", ""],
    ["Video compression", "", ""],
    ["General data compression", "", ""],
    ["Explosion modeling", "", ""],
    ["Physics: relativity", "", ""],
    ["Molecular dynamics", "", ""],
    ["Ray tracing", "", ""],
    ["Fluid dynamics", "", ""],
    ["Weather forecasting", "", ""],
    ["3D rendering and animation", "", ""],
    ["Atmosphere modeling", "", ""],
    ["Wide-scale ocean modeling (climate level)", "", ""],
    ["Image manipulation", "", ""],
    ["Computational Electromagnetics", "", ""],
    ["Regional ocean modeling", "", ""],
    ["Discrete Event simulation - computer network", "", ""],
    ["Biomedical imaging: optical tomography with finite elements", "", ""],
    ["Artificial Intelligence: alpha-beta tree search (Chess)", "", ""],
    ["Artificial Intelligence: Monte Carlo tree search (Go)", "", ""],
    ["Artificial Intelligence: recursive solution generator (Sudoku)", "", ""]
]

if __name__ == '__main__':
    st.title("基于应用领域的CPU推荐平台")
    st.subheader("选择应用领域")
    data=cpu_score.load_data()
    total_weight = 100
    col1, col2 = st.columns([1, 1])
    select = True
    with col1:
        for item in chklist[:9]:
            item[1] = st.checkbox(item[0])

    with col2:
        for item in chklist[9:18]:
            item[1] = st.checkbox(item[0])

    for item in chklist[18:24]:
        item[1] = st.checkbox(item[0])

    flg_init = False
    pre_rightend = 0
    for item in chklist:
        if item[1] == True:
            if not flg_init:
                print(item[0])
                values = st.slider("应用： " + item[0], 0, 100, (0, 0), key=item[0])
                flg_init = True
                pre_rightend = values[1]
                item[2] = values  # values[1] - values[0]
            else:
                print(item[0])
                item[2] = st.slider("应用： " + item[0], 0, 100, (pre_rightend, pre_rightend), key=item[0])
                # = values#values[1] - values[0]
                pre_rightend = item[2][1]
            st.write("所占权重：", item[2][1] - item[2][0])

    submit = {}
    apps = []
    app2score={'Perl interpreter': ['500.perlbench_r', '600.perlbench_s'],
    'GNU C compiler': ['502.gcc_r', '602.gcc_s'],
    'Route planning': ['505.mcf_r', '605.mcf_s'],
    'Discrete Event simulation - computer network': ['520.omnetpp_r',
    '620.omnetpp_s'],
    'XML to HTML conversion via XSLT': ['523.xalancbmk_r', '623.xalancbmk_s'],
    'Video compression': ['525.x264_r', '625.x264_s'],
    'Artificial Intelligence: alpha-beta tree search (Chess)': ['531.deepsjeng_r',
    '631.deepsjeng_s'],
    'Artificial Intelligence: Monte Carlo tree search (Go)': ['541.leela_r',
    '641.leela_s'],
    'Artificial Intelligence: recursive solution generator (Sudoku)': ['548.exchange2_r',
    '648.exchange2_s'],
    'General data compression': ['557.xz_r', '657.xz_s'],
    'Explosion modeling': ['503.bwaves_r', '603.bwaves_s'],
    'Physics: relativity': ['507.cactuBSSN_r', '607.cactuBSSN_s'],
    'Molecular dynamics': ['508.namd_r', '544.nab_r', '644.nab_s'],
    'Biomedical imaging: optical tomography with finite elements': ['510.parest_r'],
    'Ray tracing': ['511.povray_r'],
    'Fluid dynamics': ['519.lbm_r', '619.lbm_s'],
    'Weather forecasting': ['521.wrf_r', '621.wrf_s'],
    '3D rendering and animation': ['526.blender_r'],
    'Atmosphere modeling': ['527.cam4_r', '627.cam4_s'],
    'Wide-scale ocean modeling (climate level)': ['628.pop2_s'],
    'Image manipulation': ['538.imagick_r', '638.imagick_s'],
    'Computational Electromagnetics': ['549.fotonik3d_r', '649.fotonik3d_s'],
    'Regional ocean modeling': ['554.roms_r', '654.roms_s']}
    
    if st.button("提交"):
        for item in chklist:
            if item[1] == True:
                submit[item[0]] = float(item[2][1] - item[2][0])

        # 等待数据库返回

        with st.spinner('检索中......'):

            df = cpu_score.get_app_weight_result(data, app2score, submit)
        st.success('展示推荐Top10 CPU')

        st.table(df)



