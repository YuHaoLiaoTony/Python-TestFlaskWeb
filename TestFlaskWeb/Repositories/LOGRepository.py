from Repositories.BaseRepositories.BaseMOTO_LOANRepository import BaseMOTO_LOANRepository
from Models.LOGModel import LOGModel

class LOGRepository(BaseMOTO_LOANRepository):
    def __init__(self):
        super().__init__(LOGModel)
