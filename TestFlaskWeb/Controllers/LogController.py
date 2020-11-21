from Ext.Convert import ConvertObjExt
from Ext.Convert import ConvertArrayExt
from Models.LOGModel import LOGModel
from Repositories.LOGRepository import *
from flask import jsonify

class LogController():
    def init_app(self,app,config):
        @app.route('/logs')
        def get_logs():
            logs = LOGRepository().GetArrayToList() 
    
            #lambda x:x['LOG_TIME']  等於 C# 的 x=>x.LOG_TIME 
            #(Python 不用回傳 直接排序完了)
            logs.sort(reverse=True, key=lambda x:x.LOG_TIME)
            #restult = ConvertArrayExt(texts).ToJsonArray()
            return jsonify(ConvertArrayExt(logs).ToJsonArray())

        @app.route('/logs/<int:id>')
        def get_log(id):
            log = LOGRepository().GetOneToClass(id) 
    
            return jsonify(ConvertObjExt(log).ToJson())
