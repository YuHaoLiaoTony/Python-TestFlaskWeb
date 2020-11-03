import prediction_monitor_chailease
def init_app(app,config):
    @app.route('/')
    def auths():
        #prediction_monitor_chailease(config)
        return config
