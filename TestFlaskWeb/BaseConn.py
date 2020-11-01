import pyodbc 

class BaseConn:
    def __init__(self, server, database,username,password):
        self.Server = server
        self.Database = database
        self.Username = username
        self.Password = password
    def CreateConn(self):
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.Server+';DATABASE='+self.Database+';UID='+self.Username+';PWD='+ self.Password)
        self.Cnxn = cnxn
        self.Cursor = cnxn.cursor()
        return self.Cursor
    def __exit__(self):
        self.Cursor.close()
        self.Cnxn.close() 

