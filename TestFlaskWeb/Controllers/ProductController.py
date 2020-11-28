from flask import jsonify
from MongodbRepositories.ProductRepository import ProductRepository
from Ext.Convert import ConvertArrayExt
from Ext.Convert import ConvertObjExt
from Models.ProductModel import ProductModel
from Controllers.BaseController import BaseController

_ProductRepository = ProductRepository()

class ProductController(BaseController):
    def init_app(self,app,config):
        @app.route('/product')
        def GetProducts():
            products = _ProductRepository.GetAllToList()
            result = ConvertArrayExt(products).ListToJsonArray()
            return jsonify(result)

        @app.route('/product/<string:id>')
        def GetProduct(id):
            product = _ProductRepository.GetToClass(id)
            result = ConvertObjExt(product).ClassToJson()
            return jsonify(result)