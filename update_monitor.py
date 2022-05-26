import schedule
import requests
import urllib.request
import pandas as pd
import os
import pymysql
import xlrd
import numpy as np

# SCHEDULE - MAIN PART
def scheduleMonitor(job):
    schedule.clear()
    schedule.every().day.at("00:00").do(job)
    while True:
        schedule.run_pending()
        

categories = {'intspeed' : 'CINT2017',
              'intrate' : 'CINT2017rate',
              'fprate' : 'CFP2017rate',
              'fpspeed' : 'CFP2017'}
        
def update_all():
    # update new infos
    
    print('1. UPDATING ONLINE SCORES INFO INTO A CSV FILE...')
#     r = requests.get('https://www.spec.org/cgi-bin/osgresults?conf=cpu2017;op=dump;format=csvdump')
#     f = open('newly.csv', 'w+')
#     f.writelines(r.content.decode('utf-8').split('\',\''))
#     f.close()
    new_rs = pd.read_csv('newly.csv')
    
    # update url_posts files
    print('2. UPDATING URL_POSTS INFO...')
    for category in categories:
        print('UPDATING ' + category + '\'s URL_POSTS INFO...')
        f = open('url_posts_' + category +'.txt', 'r+')
        idx_url_pairs = f.readlines()
        last_idx = int(idx_url_pairs[-1].split(':')[0])
        idx_url_pairs = {i.split(':')[0] : i.split(':')[1].strip() for i in idx_url_pairs}
        f.close()
        old_idx_url_pairs = list(idx_url_pairs.values())
        df_tmp = new_rs[new_rs['Benchmark'] == categories[category]]
        newly_idx_url_pairs = list(df_tmp['Disclosure'].apply(
                                            lambda x: x[x.find('>HTML</A> <A HREF="') + 19:x.find('CSV') - 2]))
        newly_added = [i for i in newly_idx_url_pairs if i not in old_idx_url_pairs]
        print(str(len(newly_added)) + ' FILES TO UPDATE.')
        cnt = last_idx + 1
        newly_added_lines = []
        for i in newly_added:
            newly_added_lines.append(str(cnt) + ':' + i + '\n')
            cnt += 1
        f = open('url_posts_' + category + '.txt', 'a')
        f.writelines(newly_added_lines)
        f.close()


    # update files
    print('3. UPDATING FILES...')
    for category in categories:
        print('UPDATING ' + category + '\'s FILES...')
        f = open('url_posts_' + category + '.txt', 'r')
        url_posts = {line.split(':')[0] : line.split(':')[1].strip() for line in f.readlines()}
        f.close()
        if os.path.exists('online_' + category) == False: os.mkdir('online_' + category)
        for i in url_posts:
            if os.path.exists('online_' + category + '/' + str(i)) == False:
                os.mkdir('online_' + category + '/' + str(i))
            filename = 'CPU2017.001.' + category + '.refrate.csv' if category[-1] == 'e' else 'CPU2017.001.' + category + '.csv'
            
            if os.path.exists('online_' + category + '/' + str(i) + '/' + filename) == False:
                print(i, '/', len(url_posts))
                urllib.request.urlretrieve('http://www.spec.org' + url_posts[i], './online_' + category + '/' + i + '/' + filename)
                urllib.request.urlretrieve('http://www.spec.org' + url_posts[i][:-3] + 'cfg', './online_' + category + '/' + i + '/' + filename[:-3] + 'cfg')
                
    print('4. UPDATE ALL 4 SHEETS...')
    update_all_4_sheet()
    
    print('5. UPDATE SQL PART...')
    update_sql()
    
# COMBINE THE WHOLE SHEET
def update_all_4_sheet():
    # get all csv files
    suites = {'500.perlbench_r', '502.gcc_r', '503.bwaves_r', '505.mcf_r', '507.cactuBSSN_r', '508.namd_r', '510.parest_r', 
              '511.povray_r', '519.lbm_r', '520.omnetpp_r', '521.wrf_r', '523.xalancbmk_r', '525.x264_r', '526.blender_r', '554.roms_r',
              '527.cam4_r', '531.deepsjeng_r', '538.imagick_r', '541.leela_r', '548.exchange2_r', '557.xz_r', '544.nab_r', '549.fotonik3d_r',
              '600.perlbench_s', '602.gcc_s', '603.bwaves_s', '605.mcf_s', '607.cactuBSSN_s', '619.lbm_s',
              '620.omnetpp_s', '621.wrf_s', '623.xalancbmk_s', '625.x264_s', '627.cam4_s', '628.pop2_s', '631.deepsjeng_s', '638.imagick_s',
              '641.leela_s', '644.nab_s', '648.exchange2_s', '649.fotonik3d_s', '654.roms_s', '657.xz_s'}
    print('1. UPDATING ONLINE SCORES INFO INTO A CSV FILE...')
    file_list = {}
    for category in categories:
        filepath = 'online_' + category
        file_list[category] = {i : filepath + '/' + i + '/' + os.listdir(filepath + '/' + i)[0][:-3] + 'csv' for i in os.listdir(filepath) if i[0] != '.'}
    
    def get_scores_lines(file_list, category, file_idx):
        target_file = file_list[category][file_idx]
        f = open(target_file, 'r+', encoding='unicode-escape')
        lines = f.readlines()
        f.close()
        scores_lines = []
        for idx in range(len(lines)):
            if lines[idx] == '"Selected Results Table"\n':
                scores_lines = {i.split(',')[0] : i.split(',')[3] for i in lines[idx+3:idx+13]}
        return scores_lines
    
    idx_score_pairs = {category : {} for category in categories}
    for category in file_list:
        print(category)
        for file_idx in file_list[category]:
            idx_score_pairs[category][file_idx] = get_scores_lines(file_list, category, file_idx)
    print('DONE')
    
    url_idx_pairs = {category : {} for category in categories}
    for category in categories:
        f = open('url_posts_' + category + '.txt', 'r+')
        lines = f.readlines()
        f.close()
        url_idx_pairs[category] = {i.split(':')[1].strip(): i.split(':')[0].strip() for i in lines}
    
    df = pd.read_csv('newly.csv')
    
    dfs = []
    for category in categories:
        df_tmp = df[df['Benchmark'] == categories[category]]
        df_tmp['Scores_details'] = df_tmp['Disclosure'].apply(
                                        lambda x: 
                                        idx_score_pairs[category][
                                            url_idx_pairs[category][
                                                x[x.find('>HTML</A> <A HREF="') + 19:x.find('CSV') - 2]
                                            ]
                                        ])
        for k in suites:
            df_tmp[k] = df_tmp['Scores_details'].apply(lambda x : x[k] if k in x else np.nan)
        del df_tmp['Scores_details']
        dfs.append(df_tmp)
    
    df_all = pd.concat(dfs, axis=0)
    df_all.to_csv('added_scores_details.csv', index=False)

# UPDATE SQL
def update_sql():

    df = pd.read_csv("added_scores_details.csv")
    df=df[df.Scores_details.isnull()==False]

    df['Compiler']=df['Compiler'].apply(lambda x:x.split(':')[0])
    df=df.rename(columns={'Hardware Vendor\t':'Hardware Vendor'})
    df.fillna('NAN',inplace=True)
    df.columns=[df.columns[i].replace('#','').replace(' ','') for i in range(df.columns.shape[0])]
    df.drop('Disclosure',axis=1,inplace=True)


    col_name=["`{}` char(128)".format(df.columns[i]) for i in range(df.columns.shape[0])]
    name=["`{}`".format(df.columns[i]) for i in range(df.columns.shape[0])]
    cols=",".join(col_name)
    sql="".join("create table cpu2017( {});".format(cols))


    # In[ ]:


    conn = pymysql.connect(
            host='localhost', 
            user='root', 
            passwd='root',  
            db='data_platform',  
            port=3306,  
            charset='utf8'
            )
    # 获得游标

    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS cpu2017")
    col_name=["`{}` varchar(300)".format(df.columns[i]) for i in range(df.columns.shape[0])]
    name=["`{}`".format(df.columns[i]) for i in range(df.columns.shape[0])]
    cols=",".join(col_name)
    sql="".join("create table cpu2017( {});".format(cols))
    try:
        # 执行SQL语句
        cursor.execute(sql)
        print("创建数据库成功")
    except Exception as e:
        print("创建数据库失败：case%s"%e)
    finally:
        #关闭游标连接
        cursor.close()
        # 关闭数据库连接


    # In[ ]:


    conn = pymysql.connect(
            host='localhost', 
            user='root', 
            passwd='root', 
            db='data_platform',  
            port=3306,  
            charset='utf8'
            )
    # 获得游标
    cur = conn.cursor()
    # 创建插入SQL语句
    query = 'insert into cpu2017({}) values ({});'.format(','.join(name),','.join(['%s' for i in range(df.columns.shape[0])]))
    for r in df.iterrows():
        cur.execute(query, list(r[1].values))

    cur.close()
    conn.commit()
    conn.close()
    print('写入数据库成功')
    
# test schedule - success
scheduleMonitor(update_all)
# update_all()

