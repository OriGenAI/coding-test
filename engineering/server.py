from flask import Flask
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS

from endpoints.datasets import Datasets 
from endpoints.datasets import Dataset

app = Flask(__name__)
api = Api(app)
CORS(app)

# add resources
api.add_resource(Datasets, '/datasets')
api.add_resource(Dataset, '/datasets/<id>')

# run server on port 5000
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
