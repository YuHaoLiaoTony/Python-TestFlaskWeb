class BeforeRequestFilter():
    def init_app(self,app):
        @app.before_request
        def before_request():
            print("before_request executing!")
