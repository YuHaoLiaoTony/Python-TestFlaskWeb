from flask import Flask
import json                                                                                                                                            
import os
from os.path import dirname,basename,isfile,join
import glob
app = Flask(__name__)
#載入Config
BASE_PATH = os.path.dirname(os.path.realpath(__file__).replace('Controllers',''))
with open(BASE_PATH+'/config.json') as json_data_file:                                                                                                                            
    Config = json.load(json_data_file)
    #刪除註解
    Config.pop('\\')


modules = glob.glob(join(dirname(os.path.realpath(__file__))+'\Controllers',"*.py"))
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]

for controllerName in __all__:
    controller = __import__('Controllers.' + controllerName ,fromlist = __all__)
    controller.init_app(app,Config)
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