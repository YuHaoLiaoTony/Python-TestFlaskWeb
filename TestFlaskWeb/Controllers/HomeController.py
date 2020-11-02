import json                                                                                                                                            
import os
BASE_PATH = os.path.dirname(os.path.realpath(__file__).replace('Controllers',''))
with open(BASE_PATH+'/config.json') as json_data_file:                                                                                                                            
    Config = json.load(json_data_file)

def init_app(app):
    @app.route('/')
    def auths():
        
        return Config
