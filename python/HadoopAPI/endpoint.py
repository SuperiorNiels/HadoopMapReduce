
from flask import Flask
from flask.json import jsonify
from subprocess import call
import os, threading, time

app = Flask(__name__)
running = True
devnull = open(os.devnull, 'w')

mongo_db_ip = "Hadoop:smartcity@143.129.39.127"
mongo_db_port = "27017"
db = "Votes"
inCollection = "Votes"

command = [os.environ["HADOOP_HOME"]+ "/bin/hadoop",
           "jar",
           "map_reduce.jar",
           "MapReduce",
           "OPERATION",
           mongo_db_ip,
           mongo_db_port,
           db + "." + inCollection,
           db + "." + "OUTCOLLECTION"]

thread_lock = False

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
        print(command)
        call(command)#, stdout=devnull, stderr=devnull)
        thread_lock = False

@app.route("/countVotes")
def countVotes():
    if not thread_lock:
        thread = MapReduce("vote_count", "vote_cache")
        thread.start()
        thread.join()
        res = "done"
    else:
        res = "busy"
    return jsonify({"calculation": res})

@app.route("/countUserVotes")
def countUserVotes():
    if not thread_lock:
        thread = MapReduce("count_user_votes", "user_votes_cache")
        thread.start()
        thread.join()
        res = "done"
    else:
        res = "busy"
    return jsonify({"calculation": res})

if __name__ == '__main__':
    updater = Updater()
    updater.start()
    app.run(port=8080)
    running = False
    updater.join()
