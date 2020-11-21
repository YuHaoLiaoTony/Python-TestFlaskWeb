
class AfterRequestFilter():
    def init_app(self,app):
        @app.after_request
        def after_request(response):
            #response_value = response.get_data() 
            #print(response_value)
            #response.set_data( "test" ) 
            return response

        