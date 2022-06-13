# -*- codeing = utf-8 -*-
import schedule
import requests
import urllib.request
import pandas as pd
import os
import xlrd
import numpy as np

# MongoDB
import pymongo
import time
import pandas as pd
from threading import Thread
import matplotlib.pyplot as plt
import datetime
import json

# Clickhouse
from clickhouse_driver import Client
import os

# preprocess
import re

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
    
    print('5. UPDATE MongoDB PART...')
    update_mongodb()
    
    print('6. UPDATE Clickhouse PART...')
    update_clickhouse()
    
    
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
    df_all.rename(columns={'Hardware Vendor\t' : 'Vendor',
                           '# Cores' : 'Cores',
                           '# Chips ' : 'Chips',
                           '# Enabled Threads Per Core' : 'Threads',
                           'Processor ' : 'Processor', 'Updated ' : 'Updated'})
    df_all = pd.concat(dfs, axis=0)
    df_all.to_csv('added_scores_details.csv', index=False)

# UPDATE Clickhouse & MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='yoyo', password='090416')
mydb = myclient["spec"]
mycol = mydb["speccpu2017"]

# Categories
df_original = pd.read_csv('cpu2017-results-20220305-011913.csv')
categories = {'intspeed' : 'CINT2017',
              'intrate' : 'CINT2017rate',
              'fprate' : 'CFP2017rate',
              'fpspeed' : 'CFP2017'}

class ThreadTest(Thread):
    def __init__(self, func, args=()):
        '''
        :param func: 被测试的函数
        :param args: 被测试的函数的返回值
        '''
        super(ThreadTest, self).__init__()
        self.func = func
        self.args = args

    def run(self) -> None:
        self.result = self.func(*self.args)

    def getResult(self):
        try:
            return self.result
        except BaseException as e:
            return e.args[0]

def update_mongodb():
    # Unit function
    def insert_csv_cfg_by_url_id(url_id, mycol):
        ''' Insert csv file and cfg file in collection  in mongodb
        input:
        - url_id: https://spec.org/cpu2017/results/<url_id>.cfg
        - mongodb's collection
        '''
        if [i for i in mycol.find({'_id' : url_id}, {'csv' : 0, 'cfg' : 0})] != []:
            return (0, 0, 0, 0, True, 0) # both csv and cfg files are in the mongodb
        # print('Downloading ' + url_id + ' \'s CSV part...')
        # start = time.time()
        csv_url ='https://spec.org/cpu2017/results/' + url_id + '.csv'
        try:
            csv_req = requests.get(csv_url)
        except:
            time.sleep(5)
            csv_req = requests.get(csv_url)
        # end_csv = time.time()
        csv_lines = csv_req.content
        csv_code = csv_req.status_code
        csv_seconds= csv_req.elapsed.total_seconds()
        # print('Downloaded CSV by ' + str(end_csv - start) + ' s')

        # print('Downloading ' + url_id + ' \'s CFG part...')
        # start_cfg = time.time()
        cfg_url ='https://spec.org/cpu2017/results/' + url_id + '.cfg'
        try:
            cfg_req = requests.get(cfg_url, timeout=3)
        except:
            time.sleep(5)
            cfg_req = requests.get(cfg_url, timeout=3)
        # end_cfg = time.time()
        cfg_lines = cfg_req.content
        cfg_code = cfg_req.status_code
        cfg_seconds= cfg_req.elapsed.total_seconds()
        # print('Downloaded CFG by ' + str(end_cfg - start_cfg) + ' s')

        if csv_code != 200 or cfg_code != 200: # 如果报错就 fail
            return (csv_code, cfg_code, csv_seconds, cfg_seconds, False, 0) # if requests fail 

        # print('Inserting CSV & CFG into MongoDB...')
        start_insert = time.time()
        insert_re = mycol.insert_one({'_id' : url_id, 'csv' : csv_lines, 'cfg' : cfg_lines})
        end = time.time()
        insert_acknowledged = insert_re.acknowledged
        # print('Inserted CSV & CFG by ' + str(end - start_insert) + ' s')
        return (csv_code, cfg_code, csv_seconds, cfg_seconds, insert_acknowledged, round(end - start_insert, 6))

    def calculationTime(startTime, endTime):
        '''计算两个时间之差，单位是秒'''
        return (endTime - startTime).seconds


    def getResult(seconds):
        '''获取服务端的响应时间信息'''
        data={
          'Max' : sorted(seconds)[-1],
          'Min' : sorted(seconds)[0],
          'Median' : np.median(seconds),
          '99%Line' : np.percentile(seconds, 99),
          '95%Line' : np.percentile(seconds, 95),
          '90%Line' : np.percentile(seconds, 90)
        }
        return data

    def download_category(category, newly_idx_url_pairs):
        # count = 1000
        startTime = datetime.datetime.now()

        list_count = []
        results = []

        #失败的信息
        fails = []

        #成功任务数
        success = []

        csv_codes = []
        cfg_codes = []
        insert_codes = []

        csv_seconds = []
        cfg_seconds = []
        insert_seconds = []

        csv_sum = 0
        cfg_sum = 0
        insert_sum = 0

        for i in range(0, len(newly_idx_url_pairs), 100):
            print(i, ' - ', i + 100, '/', len(newly_idx_url_pairs), '...')
            tasks = []
            time.sleep(5)
            for url_id in newly_idx_url_pairs[i : i + 100]:
                t = ThreadTest(insert_csv_cfg_by_url_id, args=(url_id, mycol))
                tasks.append(t)
                t.start()

            for t in tasks:
                t.join()
                if t.getResult()[4] != True:
                    fails.append(t.getResult())
                results.append(t.getResult())

        endTime = datetime.datetime.now()

        for item in results:
            csv_codes.append(item[0])
            cfg_codes.append(item[1])
            csv_seconds.append(item[2])
            cfg_seconds.append(item[3])
            insert_codes.append(item[4])
            insert_seconds.append(item[5])
        for i in range(len(csv_codes)):
            list_count.append(i)

        # CSV response plot
        fig,ax = plt.subplots()
        ax.plot(list_count, csv_seconds)
        ax.set(xlabel='number of times', ylabel='Request time-consuming',
              title='CSV continuous request response time (seconds)')
        ax.grid()
        fig.savefig(category + '_csv_response.png')
        plt.show()

        # CFG response plot
        fig,ax = plt.subplots()
        ax.plot(list_count, cfg_seconds)
        ax.set(xlabel='number of times', ylabel='Request time-consuming',
              title='CFG continuous request response time (seconds)')
        ax.grid()
        fig.savefig(category + '_cfg_response.png')
        plt.show()

        # INSERT response plot
        fig,ax = plt.subplots()
        ax.plot(list_count, insert_seconds)
        ax.set(xlabel='number of times', ylabel='Request time-consuming',
              title='INSERT continuous request response time (seconds)')
        ax.grid()
        fig.savefig(category + '_insert_response.png')
        plt.show()

        # average response time
        for i in csv_seconds:
            csv_sum += i
        csv_rate = csv_sum / len(list_count)
        for i in cfg_seconds:
            cfg_sum += i
        cfg_rate = cfg_sum / len(list_count)
        for i in insert_seconds:
            insert_sum += i
        insert_rate = insert_sum / len(list_count)

        totalTime = calculationTime(startTime=startTime, endTime=endTime)

        if totalTime < 1:
            totalTime = 1

        try:
            throughput = int(len(list_count) / totalTime)
        except Exception as e:
            print(e.args[0])

        errorRate = 0

        if len(fails) == 0:
            errorRate = 0.00
        else:
            errorRate = len(fails) / len(csv_codes) * 100

        throughput = str(throughput) + '/S'

        csv_timeData = getResult(seconds=csv_seconds)
        cfg_timeData = getResult(seconds=cfg_seconds)
        insert_timeData = getResult(seconds=insert_seconds)

        dict1 = {
            '吞吐量' : throughput,
            'CSV 平均响应时间' : csv_rate,
            'CFG 平均响应时间' : cfg_rate,
            'INSERT 平均响应时间' : insert_rate,
            'CSV 响应时间' : csv_timeData,
            'CFG 响应时间' : cfg_timeData,
            'INSERT 响应时间' : insert_timeData,
            '错误率' : errorRate,
            '请求总数' : len(list_count),
            '失败数' : len(fails)
        }

        print(json.dumps(dict1,indent=True,ensure_ascii=False))



    category = 'intspeed'
    for category in categories:
        print(category, ':')
        df_tmp = df_original[df_original['Benchmark'] == categories[category]]
        newly_idx_url_pairs = list(df_tmp['Disclosure'].apply(
                                            lambda x: x[x.find('>HTML</A> <A HREF="') + 36:x.find('CSV') - 6]))
        download_category(category, newly_idx_url_pairs)

# Clickhouse PART
def update_clickhouse():
    db_name = 'default'
    host='localhost' 
    port ='9000' 
    user='' 
    password='' 
    database = db_name
    send_receive_timeout = 25
    c = Client(host=host, port=port, database=database)
    c.execute('show databases')
    l = [i[0] for i in c.execute('show tables from default')]
    # test case for sql
    sqllist = ['select count(*) from default.' + i for i in l]
    for sql in sqllist:
        print(sql)
        print(c.execute(sql)[0][0])
    # Change the table's info - such as COLUMNS NAMES
    c.execute('drop table default.mach')
    c.execute('drop table default.cpu')
    c.execute('drop table default.test')
    c.execute("""CREATE TABLE default.cpu (`cpuid` UInt32, `Processor` String, `ProcessorMHz` UInt32, `CPU(s)Orderable` String,
                `Parallel` String, `BasePointerSize` String, `PeakPointerSize` String, `CoresPerCPU` Float32, `L1Dcache(KB)` Float32,
                `L1Icache(KB)` Float32, `L2cache(MB)` Float32, `L3cache(MB)` Float32, `OtherCache` String)
                ENGINE = MergeTree PRIMARY KEY `cpuid` ORDER BY `cpuid`;""")
    c.execute("""CREATE TABLE default.mach (`machid` UInt32, `Vendor` String, `System` String, `Cores` UInt32, `Chips` UInt32, `Memory` String,
                `MemoSize(GB)` Float32, `MemoNum` String, `Storage` String, `DiskSize(TB)` Float32, `SSD` String, `OperatingSystem` String,
                `OS` String, `OSVersion` String, `KernelVersion` String, `FileSystem` String, `Compiler` String, 
                `FC` String, `FV` String, `CC` String, `CV` String, `C++C` String, `C++V` String,
                `cpuid` UInt32, `cpu` String) ENGINE = MergeTree PRIMARY KEY `machid` ORDER BY `machid`;""")
    c.execute("""CREATE TABLE default.test (`Benchmark` String, `machid` UInt32, `testid` UInt32, `HW Avail` String, `SW Avail` String, 
                `License` String, `TestedBy` String, `TestSponsor` String, `TestDate` String, `Published` String, `Updated` String, 
                `PeakResult` Float32, `BaseResult` Float32, `EnergyPeakResult` Float32, `EnergyBaseResult` Float32, 
                `531.deepsjeng_r` Float32, `600.perlbench_s` Float32, `625.x264_s` Float32, `510.parest_r` Float32, `500.perlbench_r` Float32, 
                `502.gcc_r` Float32, `607.cactuBSSN_s` Float32, `623.xalancbmk_s` Float32, `619.lbm_s` Float32, `644.nab_s` Float32, `602.gcc_s` Float32, 
                `505.mcf_r` Float32, `649.fotonik3d_s` Float32, `507.cactuBSSN_r` Float32, `654.roms_s` Float32, `557.xz_r` Float32, `605.mcf_s` Float32, 
                `525.x264_r` Float32, `511.povray_r` Float32, `638.imagick_s` Float32, `621.wrf_s` Float32, `541.leela_r` Float32, `520.omnetpp_r` Float32, 
                `508.namd_r` Float32, `631.deepsjeng_s` Float32, `641.leela_s` Float32, `503.bwaves_r` Float32, `627.cam4_s` Float32, `657.xz_s` Float32, 
                `648.exchange2_s` Float32, `538.imagick_r` Float32, `523.xalancbmk_r` Float32, `548.exchange2_r` Float32, `521.wrf_r` Float32, 
                `519.lbm_r` Float32, `628.pop2_s` Float32, `526.blender_r` Float32, `603.bwaves_s` Float32, `527.cam4_r` Float32, `620.omnetpp_s` Float32) 
                ENGINE = MergeTree  ORDER BY `testid`;""")
    os.system('cat ../DWData/test.csv | clickhouse-client --query="INSERT INTO default.test FORMAT CSVWithNames";')
    os.system('cat ../DWData/cpu.csv | clickhouse-client --query="INSERT INTO default.cpu FORMAT CSVWithNames";')
    os.system('cat ../DWData/mach.csv | clickhouse-client --query="INSERT INTO default.mach FORMAT CSVWithNames";')
    print('cpu:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='cpu'")]),
          '\nmach:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='mach'")]),
           '\ntest:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='test'")]),
         '\ntags:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='tags'")]))
    requests_dict = {
            'vendor' : '',
            'kernel' : '5.0.0',
            'linux' : '',
            'cpu' : '',
            'server' : '',
            'cores' : '',
            'chips' : '',
        }
    requests_range = {
            'memomin' : '2',
            'memomax' : '',
            'hardmin' : '',
            'hardmax' : ''
    }
    havequestions = [i for i in requests_dict if requests_dict[i] != ""]
    haverange = [i for i in requests_range if requests_range[i] != ""]
    dict_name_dict = {
        'vendor' : 'Vendor',
        'kernel' : 'KernelVersion',
        'server' : 'System',
        'linux' : 'OS',
        'cpu' : 'cpu',
        'cores' : 'Cores',
        'chips' : 'Chips'
    }
    range_name_dict = {
        'memo' : 'MemoSize(GB)',
        'hard' : 'DiskSize(TB)'
    }
    q1 = ' and '.join([dict_name_dict[i] + "= '" +  requests_dict[i] + "'" for i in havequestions])
    q2_min = ' and '.join(["greaterOrEquals(`" + range_name_dict[i[:-3]] + "`, toFloat64(" + requests_range[i] + '))' for i in haverange if i[-3:] == 'min'])
    q2_max = ' and '.join(["lessOrEquals(`" + range_name_dict[i[:-3]] + "`, toFloat64(" + requests_range[i] + '))' for i in haverange if i[-3:] == 'max'])
    q = ' and '.join([i for i in [q1, q2_max, q2_min] if i != ''])
    q = "select * from default.mach where " + q if q != '' else "select * from default.mach"
    c.execute(q)
    
    # Preprocess
    def get_l1cache(data, _type):
        if _type == 'D':
            match = re.match(r".+?\+\s+(\d+)", data)
            if match:
                return match.group(1)
        else:
            match = re.match(r"\s*(\d+)", data)
            if match:
                return match.group(1)
    def get_l2cache(data):
        match = re.match(r"\s*(\d+\.?\d*)\s*(\S+)", data)
        if match:
            if match.group(2) == 'KB':
                return float(match.group(1)) / 1024
            else:
                return match.group(1)
    def get_l3cache(data):
        match = re.match(r"\s*(\d+\.?\d*)\s*(\S+)", data)
        if match:
            return match.group(1)

    pd.options.display.max_columns = None
    pd.set_option('display.min_rows',1)
    df = pd.read_csv('added_scores_details.csv', low_memory=False)
        # start = time.time()
    tmp_cpu_df = df[['System', 'Processor', 'Processor MHz', 'Chips', 'Cores', 'Threads',
                     'Base Pointer Size', 'Peak Pointer Size', 'CPU(s) Orderable',
                     'Parallel', '1st Level Cache', '2nd Level Cache', '3rd Level Cache']]
    tmp_cpu_df['CoresPerCPU'] = tmp_cpu_df['Cores'] / tmp_cpu_df['Chips']
    tmp_cpu_df['L1D cache(KB)'] = tmp_cpu_df['1st Level Cache'].apply(lambda x : get_l1cache(x, 'D'))
    tmp_cpu_df['L1I cache(KB)'] = tmp_cpu_df['1st Level Cache'].apply(lambda x : get_l1cache(x, 'I'))
    tmp_cpu_df['L2 cache(MB)'] = tmp_cpu_df['2nd Level Cache'].apply(lambda x : get_l2cache(x))
    tmp_cpu_df['L3 cache(MB)'] = tmp_cpu_df['3rd Level Cache'].apply(lambda x : get_l2cache(x))
    tmp_cpu_df = tmp_cpu_df.drop(columns=['Cores', 'Chips'])
    del tmp_cpu_df['1st Level Cache']
    del tmp_cpu_df['2nd Level Cache']
    del tmp_cpu_df['3rd Level Cache']
    # duration_1 = time.time() - start
    tmp_cpu_df.to_csv("CPU维表.csv",index=False,encoding='utf_8_sig')
    def get_mem(data):
        match = re.match(r"(\d+)\s*([A-Za-z]{2})\s*\(\s*(\d+\s*x\s*\d+)\s*([A-Za-z]{2})\s*(\d+.*x\d+)?",data)
        if match:
            vol = match.group(1)
            if match.group(2) == 'TB':
                vol = float(match.group(1)) * 1024
            elif match.group(2) == 'MB':
                vol = float(match.group(1)) / 1024
            return [vol, match.group(3)]
        else:
            print('ERROR match :', data)

    #返回列表【硬盘大小，GB/TB，是否SSD】
    def get_store(data):
        match = re.match(r".*?(\d+\.?\d*)\s*(T|G)B?", data)
        if match:
            vol = match.group(1)
            if match.group(2) == 'G':
                vol = float(match.group(1)) / 1024
            return [vol, bool(data.find('SSD'))]
        else:
            print('ERROR match :', data)

    # 返回列表【发行版本，版本号，内核版本】
    def get_op(data):
        match = re.match(r".*([345]\.\d\d?\.\d\d?)", data)
        if match:
            kernel_v = str(match.group(1))
            match = re.match(r"^([A-Za-z ]+)\s+(\d+\.?\d*)", data)
            if match:
                rel = match.group(1)
                rel_v = str(match.group(2))
                rel.replace("release", "")
                rel.replace(",", "")
                return [rel, rel_v, kernel_v]
        else:
            return data

    tmp_server_df = df[['Vendor', 'System', 'Peak Result', 'Base Result', 'Cores', 'Chips',
                        'Processor', 'Processor MHz', 'Memory', 'Storage',
                        'Operating System', 'File System', 'Compiler', 'Threads',
                        '1st Level Cache', '2nd Level Cache', '3rd Level Cache']]
    tmp_server_df['tmpMemo'] = tmp_server_df['Memory'].apply(lambda x : get_mem(x))
    tmp_server_df['MemoSize(GB)'] = tmp_server_df['tmpMemo'].apply(lambda x: x[0])
    tmp_server_df['MemoNum'] = tmp_server_df['tmpMemo'].apply(lambda x: x[1])
    del tmp_server_df['tmpMemo']
    del tmp_server_df['Memory']
    tmp_server_df['tmpDisk'] = tmp_server_df['Storage'].apply(lambda x : get_store(x))
    tmp_server_df['DiskSize(TB)'] = tmp_server_df['tmpDisk'].apply(lambda x: x[0])
    tmp_server_df['SSD'] = tmp_server_df['tmpDisk'].apply(lambda x: x[1])
    del tmp_server_df['tmpDisk']
    del tmp_server_df['Storage']
    tmp_server_df['tmpOS'] = tmp_server_df['Operating System'].apply(lambda x : get_op(x))
    tmp_server_df['OS'] = tmp_server_df['tmpOS'].apply(lambda x: x[0] if type(x) == list else x)
    tmp_server_df['OSVersion'] = tmp_server_df['tmpOS'].apply(lambda x: str(x[1]) if type(x) == list else '')
    tmp_server_df['KernelVersion'] = tmp_server_df['tmpOS'].apply(lambda x: str(x[2]) if type(x) == list else '')
    del tmp_server_df['tmpOS']
    del tmp_server_df['Operating System']
    server_df = tmp_server_df
    
    def clean_compiler(uncleaned_line):
        ''' Clean the compiler into a informal structure
        e.g.
        Input: 'C/C++: Version 19.0.5.281 of Intel, C/C++, Compiler for Linux;, Fortran: Version 19.0.5.281 of, Intel Fortran, Compiler for Linux'
        Output:
        {'Fortran': ('Intel Fortran', '19.0.5.281'),
         'C': ('Intel C/C++', '19.0.5.281'),
         'C++': ('Intel C/C++', '19.0.5.281')
         }
        '''
        if ', Fortran:' in uncleaned_line and ';, Fortran:' not in uncleaned_line:
            # print('1')
            uncleaned_line = uncleaned_line.replace(', Fortran:', ';, Fortran:')
        # print(uncleaned_line)
        tmp_line = uncleaned_line
        tmp_line = {j.split(':')[0].strip() : j.split(':')[1].strip().replace('for GCC', 'of GCC').replace('Intel, C/C++', 'Intel C/C++').replace('Intel, Fortran', 'Intel Fortran').replace('Version,', 'Version').replace('Developer, Studio', 'Developer Studio').replace('C/C++, Compiler', 'C/C++ Compiler').replace('Intel C/C++, C/C++', 'Intel C/C++').replace('GCC, the, GNU', 'GCC') for j in tmp_line.split(';,')}
        # print('2', tmp_line)
        tmp_line
        keys = tmp_line.keys()
        for k in list(keys):
            if '/' in k:
                for tmp_k in k.split('/'):
                    tmp_line[tmp_k] = tmp_line[k]
                del tmp_line[k]
        tmp_v = {}
        tmp_c = {}
        for i in tmp_line:
            tmp_v[i] = str(tmp_line[i][tmp_line[i].find('Version') + 7 : tmp_line[i].find('of')].strip())
            tmp_c[i] = tmp_line[i][tmp_line[i].find('of') + 2:].strip()
            tmp_c[i] = tmp_c[i][:tmp_c[i].find('Build')].strip() if 'Build' in tmp_c[i] else tmp_c[i].strip()
            tmp_c[i] = tmp_c[i][:tmp_c[i].find('Compiler')].strip() if 'Compiler' in tmp_c[i] else tmp_c[i].strip()
            tmp_c[i] = tmp_c[i].replace('Intel C++', 'Intel C/C++').replace('Intel C++', 'Intel C/C++').replace('Fortran, Intel Fortran', 'Intel Fortran').strip().strip(',').strip()
            if tmp_c[i] == '':
                tmp_c[i] = tmp_line[i][tmp_line[i].find('Build') + 15: tmp_line[i].find('Compiler') + 15].replace('Intel C++', 'Intel C/C++').replace('Intel C++', 'Intel C/C++').replace('Fortran, Intel Fortran', 'Intel Fortran').strip().strip(',').strip()
        tmp_line = {k : (tmp_c[k], tmp_v[k]) for k in tmp_line}
        return tmp_line
    
    uncleaned = list(server_df['Compiler'])
    test_compiler_clean_df = pd.DataFrame([clean_compiler(i) for i in uncleaned])
    test_compiler_clean_df['FC'] = test_compiler_clean_df['Fortran'].apply(lambda x: x[0])
    test_compiler_clean_df['FV'] = test_compiler_clean_df['Fortran'].apply(lambda x: x[1])
    test_compiler_clean_df['CC'] = test_compiler_clean_df['C'].apply(lambda x: x[0])
    test_compiler_clean_df['CV'] = test_compiler_clean_df['C'].apply(lambda x: x[1])
    test_compiler_clean_df['C++C'] = test_compiler_clean_df['C++'].apply(lambda x: x[0])
    test_compiler_clean_df['C++V'] = test_compiler_clean_df['C++'].apply(lambda x: x[1])
    del test_compiler_clean_df['C++']
    del test_compiler_clean_df['C']
    del test_compiler_clean_df['Fortran']
    
    server_df = server_df.join(test_compiler_clean_df)
    del server_df['Compiler']
    
    server_df = server_df#.drop_duplicates()
    server_df.to_csv("服务器维表.csv",index=False,encoding='utf_8_sig')

    vendor_df = df.iloc[:,[0,1,11,28, 29, 30, 31, 32,9,16,17,18]]
    vendor_df = pd.DataFrame(vendor_df).reset_index(drop=True)
    vendor_df = vendor_df#.drop_duplicates()
    vendor_df.to_csv("测试商维表.csv",index=False,encoding='utf_8_sig')
    
    list1 = [0, 1, 2, 11, 16, 34]
    for i in range(35,78):
        list1.append(i)
    micro_df = df.iloc[:,list1]
    micro_df.to_csv("micro维表.csv",index=False,encoding='utf_8_sig')
    
    mach=pd.read_csv('服务器维表.csv')
    cpu=pd.read_csv('CPU维表.csv')
    test=pd.read_csv('测试商维表.csv')
    train=pd.read_csv('added_scores_details.csv',low_memory=True)
    mach_col=[x for x in mach.columns if x not in (list(train.columns)+['1st Level Cache.1',
           '2nd Level Cache.1', '3rd Level Cache.1'])]
    cpu_col=[x for x in cpu.columns if x not in (list(train.columns)+['1st Level Cache.1',
           '2nd Level Cache.1', '3rd Level Cache.1'])]
    train=pd.concat((train, mach[mach_col], cpu[cpu_col]), axis=1)
    mach=train[['Vendor', 'System', 'Cores', 'Chips',
                'Threads', 'Processor', 'Processor MHz',
                'CPU(s) Orderable', 'Parallel', 'Base Pointer Size',
                'Peak Pointer Size', '1st Level Cache', '2nd Level Cache',
                '3rd Level Cache', 'Other Cache', 'Memory', 'Storage',
                'Operating System', 'File System', 'Compiler', 'MemoSize(GB)',
                'MemoNum', 'DiskSize(TB)', 'SSD', 'OS', 'OSVersion', 'KernelVersion',
                'FC', 'FV', 'CC', 'CV', 'C++C', 'C++V'
           ]].drop_duplicates().reset_index(drop=True)
    mach['machid']=mach.index
    cpu=train[['Processor', 'Processor MHz',
           'CPU(s) Orderable', 'Parallel', 'Base Pointer Size',
           'Peak Pointer Size', 'CoresPerCPU', 'L1D cache(KB)', 'L1I cache(KB)',
           'L2 cache(MB)', 'L3 cache(MB)',  'Other Cache']].drop_duplicates().reset_index(drop=True)
    cpu['cpuid'] = cpu.index
    test=train[['Tested By',
                'Test Sponsor']].drop_duplicates().reset_index(drop=True)
    test['testid']=test.index
    mach_col=[x for x in mach.columns if x in train.columns]
    train=train.merge(mach, on=mach_col, how='left')
    cpu_col=[x for x in cpu.columns if x in train.columns]
    train=train.merge(cpu, on=cpu_col, how='left')
    test_col=[x for x in test.columns if x in train.columns]
    train=train.merge(test, on=test_col, how='left')
    
    mach=train[['machid','Vendor', 'System', 'Cores',
                'Chips',  'Memory', 'MemoSize(GB)', 'MemoNum', 'Storage', 'DiskSize(TB)', 'SSD',
                'Operating System', 'OS', 'OSVersion', 'KernelVersion', 'File System', 'Compiler',
                'FC', 'FV', 'CC', 'CV', 'C++C', 'C++V', 'cpuid'
           ]].drop_duplicates().reset_index(drop=True)
    cpu=train[['cpuid', 'Processor', 'Processor MHz',
               'CPU(s) Orderable', 'Parallel', 'Base Pointer Size',
               'Peak Pointer Size', 'CoresPerCPU', 'L1D cache(KB)', 'L1I cache(KB)',
               'L2 cache(MB)', 'L3 cache(MB)', 'Other Cache'
              ]].drop_duplicates().reset_index(drop=True)
    test=train[['Benchmark', 'machid', 'testid', 'HW Avail', 'SW Avail',
                'License', 'Tested By', 'Test Sponsor', 'Test Date', 'Published', 'Updated',
                'Peak Result', 'Base Result', 'Energy Peak Result', 'Energy Base Result']]
    micro=pd.read_csv('micro维表.csv')
    
    test=pd.concat((test,micro[['531.deepsjeng_r', '600.perlbench_s', '625.x264_s',
           '510.parest_r', '500.perlbench_r', '502.gcc_r', '549.fotonik3d_r',
           '554.roms_r', '607.cactuBSSN_s', '623.xalancbmk_s', '619.lbm_s',
           '644.nab_s', '602.gcc_s', '505.mcf_r', '649.fotonik3d_s',
           '507.cactuBSSN_r', '654.roms_s', '557.xz_r', '605.mcf_s', '525.x264_r',
           '511.povray_r', '638.imagick_s', '621.wrf_s', '544.nab_r',
           '541.leela_r', '520.omnetpp_r', '508.namd_r', '631.deepsjeng_s',
           '641.leela_s', '503.bwaves_r', '627.cam4_s', '657.xz_s',
           '648.exchange2_s', '538.imagick_r', '523.xalancbmk_r',
           '548.exchange2_r', '521.wrf_r', '519.lbm_r', '628.pop2_s',
           '526.blender_r', '603.bwaves_s', '527.cam4_r', '620.omnetpp_s']]),axis=1)
    
    test=test[['Benchmark', 'machid', 'testid','HW Avail', 'SW Avail', 'License',
            'Tested By', 'Test Sponsor', 'Test Date', 'Published', 'Updated',
            'Peak Result', 'Base Result', 'Energy Peak Result',
            'Energy Base Result', '531.deepsjeng_r', '600.perlbench_s', '625.x264_s',
            '510.parest_r', '500.perlbench_r', '502.gcc_r', 
            '607.cactuBSSN_s', '623.xalancbmk_s', '619.lbm_s',
            '644.nab_s', '602.gcc_s', '505.mcf_r', '649.fotonik3d_s',
            '507.cactuBSSN_r', '654.roms_s', '557.xz_r', '605.mcf_s', '525.x264_r',
            '511.povray_r', '638.imagick_s', '621.wrf_s', 
            '541.leela_r', '520.omnetpp_r', '508.namd_r', '631.deepsjeng_s',
            '641.leela_s', '503.bwaves_r', '627.cam4_s', '657.xz_s',
            '648.exchange2_s', '538.imagick_r', '523.xalancbmk_r',
            '548.exchange2_r', '521.wrf_r', '519.lbm_r', '628.pop2_s',
            '526.blender_r', '603.bwaves_s', '527.cam4_r', '620.omnetpp_s']].drop_duplicates().reset_index(drop=True)
    
    test.replace('NC',np.nan,inplace=True)
    for col in test.columns[-40:]:
        test[col]=test[col].astype(float)
        
    # Add cpu.cpu by cpuid
    join = mach.join(cpu[['cpuid', 'Processor']], on='cpuid', how='left', lsuffix='_left', rsuffix='_right')
    mach['cpu'] = join['Processor']

    # Change columns' names
    cpu.rename(columns={'Processor MHz' : 'ProcessorMHz', 
                        'CPU(s) Orderable' : 'CPU(s)Orderable',
                        'Base Pointer Size' : 'BasePointerSize',
                        'Peak Pointer Size' : 'PeakPointerSize',
                        'L1D cache(KB)' : 'L1Dcache(KB)',
                        'L1I cache(KB)' : 'L1Icache(KB)',
                        'L2 cache(MB)' : 'L2cache(MB)',
                        'L3 cache(MB)' : 'L3cache(MB)',
                        'Other Cache' : 'OtherCache'
                        }, inplace=True)
    mach.rename(columns={'Operating System' : 'OperatingSystem', 'File System' : 'FileSystem'}, inplace=True)
    test.rename(columns={'Test Sponsor' : 'TestSponsor', 'Test Date' : 'TestDate', 'Tested By' : 'TestedBy', 'Peak Result' : 'PeakResult', 'Base Result' : 'BaseResult',
                         'Energy Peak Result' : 'EnergyPeakResult', 'Energy Base Result' : 'EnergyBaseResult'
                        }, inplace=True)
    
    mach['OSVersion'] = mach['OSVersion'].apply(lambda x: str(x))
    test['License'] = test['License'].apply(lambda x: str(x))
    
    cpu.to_csv('./cpu.csv',index=False)
    mach.to_csv('./mach.csv',index=False)
    test.to_csv('./test.csv',index=False)

    # Into Clickhouse
    # Change the table's info - such as COLUMNS NAMES
    c.execute('drop table default.mach')
    c.execute('drop table default.cpu')
    c.execute('drop table default.test')
    c.execute("""CREATE TABLE default.cpu (`cpuid` UInt32, `Processor` String, `ProcessorMHz` UInt32, `CPU(s)Orderable` String,
                `Parallel` String, `BasePointerSize` String, `PeakPointerSize` String, `CoresPerCPU` Float32, `L1Dcache(KB)` Float32,
                `L1Icache(KB)` Float32, `L2cache(MB)` Float32, `L3cache(MB)` Float32, `OtherCache` String)
                ENGINE = MergeTree PRIMARY KEY `cpuid` ORDER BY `cpuid`;""")
    c.execute("""CREATE TABLE default.mach (`machid` UInt32, `Vendor` String, `System` String, `Cores` UInt32, `Chips` UInt32, `Memory` String,
                `MemoSize(GB)` Float32, `MemoNum` String, `Storage` String, `DiskSize(TB)` Float32, `SSD` String, `OperatingSystem` String,
                `OS` String, `OSVersion` String, `KernelVersion` String, `FileSystem` String, `Compiler` String, 
                `FC` String, `FV` String, `CC` String, `CV` String, `C++C` String, `C++V` String,
                `cpuid` UInt32, `cpu` String) ENGINE = MergeTree PRIMARY KEY `machid` ORDER BY `machid`;""")
    c.execute("""CREATE TABLE default.test (`Benchmark` String, `machid` UInt32, `testid` UInt32, `HW Avail` String, `SW Avail` String, 
                `License` String, `TestedBy` String, `TestSponsor` String, `TestDate` String, `Published` String, `Updated` String, 
                `PeakResult` Float32, `BaseResult` Float32, `EnergyPeakResult` Float32, `EnergyBaseResult` Float32, 
                `531.deepsjeng_r` Float32, `600.perlbench_s` Float32, `625.x264_s` Float32, `510.parest_r` Float32, `500.perlbench_r` Float32, 
                `502.gcc_r` Float32, `607.cactuBSSN_s` Float32, `623.xalancbmk_s` Float32, `619.lbm_s` Float32, `644.nab_s` Float32, `602.gcc_s` Float32, 
                `505.mcf_r` Float32, `649.fotonik3d_s` Float32, `507.cactuBSSN_r` Float32, `654.roms_s` Float32, `557.xz_r` Float32, `605.mcf_s` Float32, 
                `525.x264_r` Float32, `511.povray_r` Float32, `638.imagick_s` Float32, `621.wrf_s` Float32, `541.leela_r` Float32, `520.omnetpp_r` Float32, 
                `508.namd_r` Float32, `631.deepsjeng_s` Float32, `641.leela_s` Float32, `503.bwaves_r` Float32, `627.cam4_s` Float32, `657.xz_s` Float32, 
                `648.exchange2_s` Float32, `538.imagick_r` Float32, `523.xalancbmk_r` Float32, `548.exchange2_r` Float32, `521.wrf_r` Float32, 
                `519.lbm_r` Float32, `628.pop2_s` Float32, `526.blender_r` Float32, `603.bwaves_s` Float32, `527.cam4_r` Float32, `620.omnetpp_s` Float32) 
                ENGINE = MergeTree  ORDER BY `testid`;""")
    os.system('cat ./test.csv | clickhouse-client --query="INSERT INTO default.test FORMAT CSVWithNames";')
    os.system('cat ./cpu.csv | clickhouse-client --query="INSERT INTO default.cpu FORMAT CSVWithNames";')
    os.system('cat ./mach.csv | clickhouse-client --query="INSERT INTO default.mach FORMAT CSVWithNames";')
    
    # Show all columns information
    print('cpu:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='cpu'")]),
          '\nmach:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='mach'")]),
           '\ntest:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='test'")]),
         '\ntags:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='tags'")]))
    
    # Update Tags
    micro=['648.exchange2_s', '607.cactuBSSN_s', '505.mcf_r', '508.namd_r',
       '627.cam4_s', '619.lbm_s', '631.deepsjeng_s',
       '526.blender_r', '520.omnetpp_r', '511.povray_r', '500.perlbench_r',
       '507.cactuBSSN_r', '523.xalancbmk_r',
       '510.parest_r', '603.bwaves_s', '649.fotonik3d_s', '557.xz_r',
       '527.cam4_r', '531.deepsjeng_r', '623.xalancbmk_s', '525.x264_r',
       '605.mcf_s', '644.nab_s', '519.lbm_r', '538.imagick_r', '641.leela_s',
       '628.pop2_s', '638.imagick_s', '541.leela_r', '625.x264_s', '602.gcc_s',
       '600.perlbench_s', '620.omnetpp_s', '621.wrf_s', '657.xz_s',
       '521.wrf_r', '502.gcc_r', '503.bwaves_r', '654.roms_s']
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
        'Artificial Intelligence: recursive solution generator (Sudoku)': ['648.exchange2_s'],
        'General data compression': ['557.xz_r', '657.xz_s'],
        'Explosion modeling': ['503.bwaves_r', '603.bwaves_s'],
        'Physics: relativity': ['507.cactuBSSN_r', '607.cactuBSSN_s'],
        'Molecular dynamics': ['508.namd_r', '644.nab_s'],
        'Biomedical imaging: optical tomography with finite elements': ['510.parest_r'],
        'Ray tracing': ['511.povray_r'],
        'Fluid dynamics': ['519.lbm_r', '619.lbm_s'],
        'Weather forecasting': ['521.wrf_r', '621.wrf_s'],
        '3D rendering and animation': ['526.blender_r'],
        'Atmosphere modeling': ['527.cam4_r', '627.cam4_s'],
        'Wide-scale ocean modeling (climate level)': ['628.pop2_s'],
        'Image manipulation': ['538.imagick_r', '638.imagick_s'],
        'Computational Electromagnetics': ['649.fotonik3d_s'],
        'Regional ocean modeling': [ '654.roms_s']}
    app2tag = {
        "Perl interpreter": 'perl_tag',
        "GNU C compiler": 'gnu_tag',
        "Route planning": 'route_tag',
        "Discrete Event simulation - computer network": 'discrete_tag',
        "XML to HTML conversion via XSLT": 'xml_tag',
        "Video compression": 'video_tag',
        "Artificial Intelligence: alpha-beta tree search (Chess)": 'aichess_tag',
        "Artificial Intelligence: Monte Carlo tree search (Go)": 'aimc_tag',
        "Artificial Intelligence: recursive solution generator (Sudoku)": 'aisudoku_tag',
        "General data compression": 'compression_tag',
        "Explosion modeling": 'explosion_tag',
        "Physics: relativity": 'physics_tag',
        "Molecular dynamics": 'molecular_tag',
        "Biomedical imaging: optical tomography with finite elements": 'biomedical_tag',
        "Ray tracing": 'ray_tag',
        "Fluid dynamics": 'fluid_tag',
        "Weather forecasting": 'weather_tag', 
        "3D rendering and animation": 'render_3D_tag',
        "Atmosphere modeling": 'atmosphere_tag',
        "Wide-scale ocean modeling (climate level)": 'climateocean_tag',
        "Image manipulation": 'image_tag',
        "Computational Electromagnetics": 'electro_tag',
        "Regional ocean modeling": 'regionalocean_tag'
    }
    c.execute("""drop  table if exists tmp""")
    c.execute("""drop  table if exists tmp2""")
    c.execute("""drop  table if exists tmp3""")
    c.execute("""drop  table if exists tags""")
    
    c.execute("""CREATE TABLE default.tmp   ENGINE = Memory AS SELECT * FROM default.test T left join default.mach M
                    on T.machid=M.machid left join default.cpu C on M.cpuid=C.cpuid""")
    print("""CREATE TABLE default.tmp ENGINE = Memory AS SELECT * FROM default.test T left join default.mach M on T.machid=M.machid left join default.cpu C on M.cpuid=C.cpuid""")

    # tmp2:cpu分组后micro均值 - yoyo: 使用了几何平均
    tmp=','.join(['''pow(arrayProduct(arrayFilter(x -> x != 0, groupArray(divide(`T.{}`, `M.Chips`)))), 1 / length(arrayFilter(x -> x != 0, groupArray(`T.{}`)))) `{}_geomean` '''.format(col,col,col) for col in micro])
    c.execute("""CREATE TABLE tmp2 ENGINE = Memory AS SELECT `C.Processor`,{} FROM tmp GROUP BY `C.Processor`""".format(tmp))
    print("""CREATE TABLE tmp2 ENGINE = Memory AS SELECT `C.Processor`,{} FROM tmp GROUP BY `C.Processor`""".format(tmp))

    # tmp3:cpu分组后app score均值
    tmp=','.join([' + '.join(['`{}_geomean`'.format(x) for x in app2score[k]])+' `{}`'.format(k) for k in app2score.keys()])
    c.execute("""CREATE TABLE tmp3 ENGINE = Memory AS select `C.Processor`,{} from tmp2;""".format(tmp))
    print("""CREATE TABLE tmp3 ENGINE = Memory AS select `C.Processor`,{} from tmp2;""".format(tmp))
    
    print('Show the infos of 3 tmp tables')
    print('tmp:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='tmp'")]),
          '\ntmp2:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='tmp2'")]),
           '\ntmp3:\t', ', '.join([i[0] for i in c.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='tmp3'")]))
    
    
    q1 = ','.join([''' if(`{}` =='{}', 1, 0) as {} '''.format(app2tag[app_name], app2tag[app_name], app2tag[app_name]) for app_name in app2tag])
    q2 = ' left outer join '.join(['''
        (SELECT `C.Processor`, `{}`, '{}' as {}
            FROM tmp3 where `{}` > 
            (select quantile(0.9)(`{}`) from tmp3)
        ) as {}_R on L.`C.Processor` == {}_R.`C.Processor`
    '''.format(app_name, app2tag[app_name], app2tag[app_name], app_name, app_name, app2tag[app_name], app2tag[app_name]) for app_name in app2tag])
    q = '''create table tags ENGINE = Memory as select L.`C.Processor`,'''
    q += q1
    q += '''from tmp3 as L left outer join'''
    q += q2
    q += ''' settings join_use_nulls = 1;'''
    print(q)
    c.execute(q)



# test schedule - success
# update_all()
scheduleMonitor(update_all)