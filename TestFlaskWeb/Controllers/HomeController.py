from Ext.Convert import ConvertObjExt
from Models.ConfigModel import ConfigModel
def init_app(app,config):
    @app.route('/')
    def auths():
        t = ConvertObjExt(config).ToClass(ConfigModel)
        return config.storage_account_name
