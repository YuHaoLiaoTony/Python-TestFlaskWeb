from MongodbRepositories.Base.BaseMasterDBMongodbRepository import BaseMasterDBMongodbRepository
from Models.ProductModel import ProductModel
class ProductRepository(BaseMasterDBMongodbRepository):
    def __init__(self):
        super().__init__(ProductModel)