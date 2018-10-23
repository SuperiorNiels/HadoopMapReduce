
import os
from flask import Flask, request
from flask_restful import Resource, Api
from flask.json import jsonify
from subprocess import call

app = Flask(__name__)
api = Api(app)

os.environ["JAVA_HOME"] = "/home/niels/Programs/jdk1.8.0_181"
os.environ["HADOOP_HOME"] = "/usr/local/hadoop"

mongo_db_ip = "localhost"
mongo_db_port = "27017"
db = "votes-test"
inCollection = "inventory"
outCollection = "cache"

command = [os.environ.get("HADOOP_HOME") + "/bin/hadoop",
           "jar",
           "OPERATION",
           "MongoDBConnector",
           mongo_db_ip,
           mongo_db_port,
           db + "." + inCollection,
           db + "." + outCollection]

class Votes(Resource):
    def get(self):
        """ Start hadoop Map Reduce
            :return: json result (calculation done) """
        command[2] = "operations/countVotes.jar"
        call(command)
        res = {
            "calculation": "ready",
        }
        return jsonify(res)

api.add_resource(Votes, '/countVotes')
#api.add_resource(Votes, '/topUser')
#api.add_resource(Votes, '/')


if __name__ == '__main__':
    app.run(port=8080)