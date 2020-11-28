from flask import jsonify

from Controllers.BaseController import BaseController

class HomeController(BaseController):
    
    def init_app(self,app,config):
        @app.route('/')
        def getlist():
            return 'OK'
        


    
    
    