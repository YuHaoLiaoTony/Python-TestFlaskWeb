from pymongo import MongoClient
from bson.objectid import ObjectId
from Ext.Convert import ConvertArrayExt
from Ext.Convert import ConvertObjExt

class BaseMongodbRepository():
    def __init__(self,server,database,type):     
        self.__Client__ = MongoClient(server)
        self.__DB__ = self.__Client__[database]
        self.__Type__ = type
        self.__TableName__ = type.__name__.replace("Model", "");
        self.__Collection__ = self.__DB__[self.__TableName__]
    
    def GetAll(self):
        query = self.__Collection__.find()        
        result = []
        for p in query:
            result.append(p)
        return result
    
    def Get(self,id):
        return self.__Collection__.find_one(ObjectId(id))
    
    def Update(self,obj):
        keyName = self.__Type__.__Key__
        query = { keyName : getattr(obj,keyName) }
        data ={}
        for item in obj.__dict__:
            data[item] = getattr(obj,item)
        newvalues = { "$set": data }
        self.__Collection__.update_one(query, newvalues)\
        
    def Delete(self,id):
        query = { self.__Type__.__Key__: id }
        self.__Collection__.delete_one(query)
        
    def DeleteByIds(self,ids):
        objIds = [ObjectId(id) for id in ids]
        query = { self.__Type__.__Key__: {"$in":objIds} }
        self.__Collection__.delete_many(query)   
        
    def Insert(self,obj):
        data = self.__ToDic__(obj)
        id = self.__Collection__.insert_one(data).inserted_id
        return str(id)
    
    def InsertMany(self,list):
        
        mylist = []
        
        for obj in list:
            data = self.__ToDic__(obj)
            mylist.append(data)
            
        self.__Collection__.insert_many(mylist)

    def GetToClass(self,id):
        data = self.Get(id)
        return self.__ToClass__(data)

    def GetAllToList(self):
        query = self.__Collection__.find()        
        result = []
        for item in query:
            p = self.__ToClass__(item)
            result.append(p)
        return result
    def __ToDic__(self,obj):
        data ={}
        for item in obj.__dict__:
            data[item] = getattr(obj,item)
        #新增的時候不能有 _id 因為是 MongoDB 原生的 Key
        if hasattr(data,self.__Type__.__Key__):
            data.pop(self.__Type__.__Key__)
        return data
    
    def __ToClass__(self,data):
        result = self.__Type__()
        
        for key,value in data.items():
            if hasattr(result,key):
                setattr(result,key,value)
                
        return result
       
