from flask import Flask
from .blueprints import apiBlueprint
from flasgger import Swagger


app = Flask(__name__)

app.register_blueprint(apiBlueprint)

swagger = Swagger(app)
