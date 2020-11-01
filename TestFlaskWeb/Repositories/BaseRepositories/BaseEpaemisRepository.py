from Repositories.BaseRepositories.BaseRepository import BaseRepository
class BaseEpaemisRepository(BaseRepository):
    def __init__(self,type):
        super().__init__('leads.tw','epaemis_local_PROD','sa','VisionGood!',type)