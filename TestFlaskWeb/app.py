from flask import Flask
#import Controller
from Controllers import HomeController
from Controllers import LogController

app = Flask(__name__)

HomeController.init_app(app)
LogController.init_app(app)

wsgi_app = app.wsgi_app

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)