from flask import Flask
import json                                                                                                                                            
import os
from os.path import dirname,basename,isfile,join
import glob
from flask import send_from_directory  
from Models.ConfigModel import ConfigModel
from Ext.Convert import ConvertObjExt
from werkzeug . wrappers import Response

app = Flask(__name__ )

class  JSONResponse ( Response ) : 
     default_mimetype =  'application/json'

     @classmethod
     def force_type ( cls , response , environ = None ) : 
         if  isinstance ( response , dict ) : 
             response =  jsonify ( response ) 
         return  super ( JSONResponse , cls ) . force_type ( response , environ )

app.response_class = JSONResponse

#註冊 filter
modules = glob.glob(join(dirname(os.path.realpath(__file__))+'\Filters',"*.py"))
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]
for filterName in __all__:
    filterFile = __import__(f'Filters.{filterName}' ,fromlist = __all__)
    filter = getattr(filterFile, filterName)
    filter().init_app(app)

#載入Config
BASE_PATH = os.path.dirname(os.path.realpath(__file__))
with open(BASE_PATH+'/config.json') as json_data_file:                                                                                                                            
    JsonConfig = json.load(json_data_file)
    #刪除註解
    JsonConfig.pop('\\')
    
    Config = ConvertObjExt(JsonConfig).ToClass(ConfigModel)
#註冊 controller
modules = glob.glob(join(dirname(os.path.realpath(__file__))+'\Controllers',"*.py"))
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]
for controllerName in __all__:
    controllerFile = __import__(f'Controllers.{controllerName}' ,fromlist = __all__)
    controller = getattr(controllerFile, controllerName)
    controller().init_app(app,Config)
    pass

wsgi_app = app.wsgi_app

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
