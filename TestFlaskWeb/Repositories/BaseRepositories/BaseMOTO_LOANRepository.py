from Repositories.BaseRepositories.BaseRepository import BaseRepository
class BaseMOTO_LOANRepository(BaseRepository):
    def __init__(self,type):
        server = 'CHSQLUAT01'
        database = 'MOTO_LOAN'
        username = 'MOTO_LOAN_user'
        password = 'MOTO_LOAN_user'
        super().__init__(server,database,username,password,type)