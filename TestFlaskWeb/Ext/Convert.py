import json
from datetime import date, datetime
from collections import namedtuple
class AdvancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        return obj.__dict__

class ConvertObjExt:
    def __init__(self, obj):
        self.Obj = obj

    def ToString(self):
        #將 Class 物件轉成 JsonStr
        str = json.dumps(self.Obj, cls=AdvancedJSONEncoder)
        return str
    def ToJson(self):
        str = self.ToString()
        #將 JsonStr 轉成 數據格式
        return json.loads(str)

    def ToClass(self):
        str = self.ToString()
        obj = json.loads(str, object_hook=self.__customDecoder__)
        return obj

    def __customDecoder__(self,obj):
        return namedtuple(type(obj).__name__, obj.keys())(*obj.values())



    

    