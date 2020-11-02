"""
This script runs the application using a development server.
It contains the definition of routes and views for the application.
"""
from Models.LOGModel import LOGModel
from Repositories.LOGRepository import *
from flask import Flask
from flask import jsonify
import json
from Ext.Convert import ConvertObjExt
from Ext.Convert import ConvertArrayExt
app = Flask(__name__)

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app

@app.route('/blogs')
def get_logs():
    logs = LOGRepository().GetArrayToList() 
    
    #lambda x:x['LOG_TIME']  等於 C# 的 x=>x.LOG_TIME 
    #(Python 不用回傳 直接排序完了)
    logs.sort(reverse=True, key=lambda x:x.LOG_TIME)
    #restult = ConvertArrayExt(texts).ToJsonArray()
    return jsonify(ConvertArrayExt(logs).ToJsonArray())

@app.route('/blogs/<int:id>')
def get_log(id):
    log = LOGRepository().GetOneToClass(id) 
    
    return jsonify(ConvertObjExt(log).ToJson())

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
