from Repositories.BaseRepositories.BaseEpaemisRepository import BaseEpaemisRepository
from Models.Text import Text
class TextRepository(BaseEpaemisRepository):
    def __init__(self):
        
        super().__init__(Text)
