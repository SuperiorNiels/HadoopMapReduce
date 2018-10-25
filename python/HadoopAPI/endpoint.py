
from flask import Flask
from flask.json import jsonify
from subprocess import call
import os, threading, time

app = Flask(__name__)
running = True
devnull = open(os.devnull, 'w')

mongo_db_ip = "localhost"
mongo_db_port = "27017"
db = "votes-test"
inCollection = "inventory"

command = [os.environ["HADOOP_HOME"]+ "/bin/hadoop",
           "jar",
           "map_reduce.jar",
           "MapReduce",
           "OPERATION",
           mongo_db_ip,
           mongo_db_port,
           db + "." + inCollection,
           db + "." + "OUTCOLLECTION"]

class Updater(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        counter = 0
        while running:
            # recalculate every 10 s
            if counter == 10:
                print("Check.")
                counter = 0
            time.sleep(1)
            counter = counter + + 1

class MapReduce(threading.Thread):
    def __init__(self, operation, outCollection):
        threading.Thread.__init__(self)
        self.operation = operation
        self.outCollection = outCollection
    def setOperation(self, new_operation):
        self.operation = new_operation
    def setOutCollection(self, new_collection):
        self.outCollection = new_collection
    def run(self):
        command[4] = self.operation
        command[8] = db + "." + self.outCollection
        print("Executing: " + self.operation + ", saving to collection: " + self.outCollection)
        call(command, stdout=devnull, stderr=devnull)

@app.route("/countVotes")
def countVotes():
    thread = MapReduce("vote_count", "out")
    thread.start()
    thread.join()
    return jsonify({"calculation": "done"})

if __name__ == '__main__':
    updater = Updater()
    updater.start()
    app.run(port=8080)
    running = False
    updater.join()
