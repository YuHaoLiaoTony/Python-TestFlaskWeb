"""
This script runs the application using a development server.
It contains the definition of routes and views for the application.
"""
from Repositories.TextRepository import TextRepository

from flask import Flask
from flask import jsonify

import json
from Ext.Convert import ConvertObjExt
from Models.Text import Text
app = Flask(__name__)

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app

@app.route('/')
def hello():
    text = TextRepository().GetOne(14)
    return jsonify(text)
    #轉強行別
    result = Text(*ConvertObjExt(text).ToClass())
    
    return jsonify(ConvertObjExt(result).ToJson())



if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
