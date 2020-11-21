from Ext.Convert import ConvertArrayExt
from Models.LOGModel import LOGModel
from flask import jsonify

class HomeController():
    def init_app(self,app,config):
        @app.route('/')
        def auths():
            log = LOGModel('1','2','3','4','5','6')
            result = []
            result.append(log)
            result.append(log)

            if(type(result) == type([])):
                return jsonify(ConvertArrayExt(result).ToJsonArray())
            return log.__dict__