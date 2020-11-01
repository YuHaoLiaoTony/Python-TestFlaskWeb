from BaseConn import BaseConn
import sys

class BaseRepository:
    def __init__(self, server, database,username,password,obj):
        self.__Conn__ =  BaseConn(server,database,username,password)
        self.__Cursor__ = self.__Conn__.CreateConn()
        self.__Obj__ = obj
        self.__TableName__ = obj.__name__

    def GetList(self):
        sql = f"SELECT TOP 100* FROM {self.__TableName__}"
        result = self.__QueryList__(sql)
        return result 

    def GetOne(self,id):
        key = 'Id'
        if hasattr(self.__Obj__, '__Key__'):
            key = self.__Obj__.__Key__
        sql = f"SELECT * FROM {self.__TableName__} Where {key} = {id}"
        result = self.__QueryOne__(sql)
        return result 

    def __QueryList__(self,sql):
        result = []
        cursor = self.__Cursor__.execute(sql)
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            result.append(dict(zip(columns, row)))
        return result

    def __QueryOne__(self,sql):
        #iter 之後才可以用 next
        arr = iter(self.__QueryList__(sql))
        #next 獲得下一個值，如果沒有救回傳 None (當 FirstOrDefault 用)
        result = next(arr,None)
        return result
        