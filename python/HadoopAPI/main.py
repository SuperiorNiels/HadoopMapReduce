
from flask import Flask
from flask.json import jsonify
from subprocess import call
import os, threading
app = Flask(__name__)

mongo_db_ip = "localhost"
mongo_db_port = "27017"
db = "votes-test"
inCollection = "inventory"
outCollection = "cache"

command = [os.environ["HADOOP_HOME"]+ "/bin/hadoop",
           "jar",
           "map_reduce.jar",
           "MapReduce",
           "OPERATION",
           mongo_db_ip,
           mongo_db_port,
           db + "." + inCollection,
           db + "." + outCollection]

class MapReduce(threading.Thread):
    def __init__(self, operation):
        threading.Thread.__init__(self)
        self.operation = operation
    def setOperation(self, new_operation):
        self.operation = new_operation
    def run(self):
        command[4] = self.operation
        call(command)

@app.route("/countVotes")
def countVotes():
    """ Start hadoop Map Reduce
        :return: json result (calculation done) """
    thread = MapReduce("vote_count")
    thread.start()
    return jsonify({"calculation": "started"})

if __name__ == '__main__':
    app.run(port=8080)
