from Repositories.BaseRepositories.BaseEpaemisRepository import BaseEpaemisRepository
from Models.News import News

class NewsRepository(BaseEpaemisRepository):
    def __init__(self):
        super().__init__(News)