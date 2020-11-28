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
    def ClassToJson(self):
        dtc = {}
        if(hasattr(self.Obj,"__all__")):
            dtc = self.Obj.__all__()
        else:
            dtc = self.Obj.__dict__

        return dtc

    def JsonToClass(self,classType):
        obj = classType()
        for key,value in self.Obj.items():
            if hasattr(obj,key):
                setattr(obj,key,value)
        return obj


class ConvertArrayExt():
    def __init__(self, list):
        self.List = list

    def ListToJsonArray(self):
        mylist = []
        
        for obj in self.List:
            data = ConvertObjExt(obj).ClassToJson()
            mylist.append(data)
        return mylist

    def JsonArrayToList(self,classType):
        arr = []
        for item in self.List:
            obj = ConvertObjExt(item).JsonToClass(classType)
            arr.append(obj)
        return arr

    

    

    