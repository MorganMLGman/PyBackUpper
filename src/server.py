import logging
import logging.config
from flask import Flask, render_template

def Server():
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    return app

server = Server()
server.run(debug=True)