"""
This script runs the application using a development server.
It contains the definition of routes and views for the application.
"""
from Repositories.NewsRepository import NewsRepository

from flask import Flask
from flask import jsonify

import json
from Ext.Convert import ConvertObjExt
from Ext.Convert import ConvertArrayExt
from Models.News import News
app = Flask(__name__)

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app

@app.route('/')
def hello():
    texts = NewsRepository().GetOne(8)
    result = ConvertObjExt(texts).ToClass(News)
    
    return jsonify(ConvertObjExt(result).ToJson())



if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
