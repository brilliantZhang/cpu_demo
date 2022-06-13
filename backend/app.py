from flask import Flask, request, jsonify
import pandas as pd
import json
import numpy as np
from clickhouse_driver import Client
from flask_cors import CORS, cross_origin
import pymongo
import os

'''MongoDB Part
'''
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='yoyo', password='090416')
mydb = myclient["spec"]
mycol = mydb["speccpu2017"]


'''Clickhouse Part
'''
db_name = 'default'
host='localhost' 
port ='9000' 
user='' 
password='' 
database = db_name
send_receive_timeout = 25


cc = Client(host=host, port=port, database=database)
cc.execute('show databases')

l = [i[0] for i in cc.execute('show tables from default')]
# test case for sql
sqllist = ['select count(*) from default.' + i for i in l]
for sql in sqllist:
    print(sql)
    print(cc.execute(sql)[0][0])


'''Flask Part
'''
app = Flask(__name__)
# 跨域
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"/foo": {"origins": "http://localhost:port"}})



@app.route('/')
def index():
    return '<h1>Hello World!</h1>'

@app.route('/search', methods=['POST'])
@cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
def search():
    print('request : ', request)
    print('Form : ', request.form)
    requests_dict = {
        'vendor' : request.form.get("vendor"),
        'kernel' : request.form.get("kernel"),
        'cores' : request.form.get("cores"),
        'chips' : request.form.get("chips"),
    }
    requests_like = {
        'cpu' : request.form.get("cpu"),
        'linux' : request.form.get("linux"),
        'server' : request.form.get("server"),
    }
    requests_range = {
        'memomin' : request.form.get("memomin"),
        'memomax' : request.form.get("memomax"),
        'hardmin' : request.form.get("hardmin"),
        'hardmax' : request.form.get("hardmax")
    }
    havequestions = [i for i in requests_dict if requests_dict[i] != ""]
    havelikes = [i for i in requests_like if requests_like[i] != ""]
    haverange = [i for i in requests_range if requests_range[i] != ""]
    dict_name_dict = {
        'vendor' : 'Vendor',
        'kernel' : 'KernelVersion',
        'cores' : 'Cores',
        'chips' : 'Chips'
    }
    like_name_dict = {
        'cpu' : 'Processor',
        'linux' : 'OS',
        'server' : 'System',
    }
    range_name_dict = {
        'memo' : 'MemoSize(GB)',
        'hard' : 'DiskSize(TB)'
    }
    q1 = ' and '.join([dict_name_dict[i] + "= '" +  requests_dict[i] + "'" for i in havequestions])
    q2_min = ' and '.join(["greaterOrEquals(`" + range_name_dict[i[:-3]] + "`, toFloat64(" + requests_range[i] + '))' for i in haverange if i[-3:] == 'min'])
    q2_max = ' and '.join(["lessOrEquals(`" + range_name_dict[i[:-3]] + "`, toFloat64(" + requests_range[i] + '))' for i in haverange if i[-3:] == 'max'])
    q3 = ' and '.join([like_name_dict[i] + " like '%" +  requests_like[i] + "%'" for i in havelikes])
    q = ' and '.join([i for i in [q1, q2_max, q2_min, q3] if i != ''])
    q = "select * from default.mach where " + q + ' limit 50' if q != '' else "select * from default.mach limit 50"
    q
    re = cc.execute(q)
    # print('q :', q)
    # print('test success.')
    # print('Return :', re)
    return jsonify({"code": 200, "result": re})

# @app.route('/details', methods=['POST'])
# @cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
# def details():
#     print('request : ', request)
    

if __name__ =="__main__":
    app.run(host='0.0.0.0', debug=True)
