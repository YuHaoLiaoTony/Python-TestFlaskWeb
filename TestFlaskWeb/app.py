from flask import Flask
import json                                                                                                                                            
import os
#import Controller
from Controllers import HomeController
from Controllers import LogController

app = Flask(__name__)


#載入Config
BASE_PATH = os.path.dirname(os.path.realpath(__file__).replace('Controllers',''))
with open(BASE_PATH+'/config.json') as json_data_file:                                                                                                                            
    Config = json.load(json_data_file)
    #刪除註解
    Config.pop('\\')

HomeController.init_app(app,Config)
LogController.init_app(app,Config)



wsgi_app = app.wsgi_app

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)