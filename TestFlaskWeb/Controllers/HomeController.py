from Ext.Convert import ConvertObjExt
from Models.ConfigModel import ConfigModel
def init_app(app,config):
    @app.route('/')
    def auths():
        return config
