from MongodbRepositories.Base.BaseMongodbRepository import BaseMongodbRepository
class BaseMasterDBMongodbRepository(BaseMongodbRepository):
    def __init__(self,type):
        server = 'mongodb+srv://yuhaoliao:6quQXA0h9lGu4lP0@cluster0.anjcj.gcp.mongodb.net'
        database = 'Master'
        super().__init__(server,database,type)