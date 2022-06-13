# -*- codeing = utf-8 -*-
# @Time : 2022/6/3 19:58
# @Author : Yu
# @File : main.py
# @Software : PyCharm

from flask import Flask, render_template, request, url_for
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

@app.route("/")
def index():
    return render_template("index.html")
    
if __name__ == "__main__":
    app.run(port=9001, host='0.0.0.0', debug=True)
