
from flask import Flask
from flask.json import jsonify
from subprocess import call
import os, threading, time
import requests

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

class Updater(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        counter = 0
        while running:
            # recalculate every 5 min.
            if counter == 300:
                print("------- Automatic update started --------")
                requests.get("http://localhost:8080/countVotes")
                requests.get("http://localhost:8080/countUserVotes")
                counter = 0
            time.sleep(1)
            counter = counter + 1


mutex = threading.Lock()

class MapReduce():
    def __init__(self, operation, outCollection):
        self.operation = operation
        self.outCollection = outCollection
    def setOperation(self, new_operation):
        self.operation = new_operation
    def setOutCollection(self, new_collection):
        self.outCollection = new_collection
    def run(self):
        if not mutex.locked():
            mutex.acquire()
            command[4] = self.operation
            command[8] = db + "." + self.outCollection
            print("Executing: " + self.operation + ", saving to collection: " + self.outCollection)
            #print(command)
            call(command, stdout=devnull, stderr=devnull)
            mutex.release()
            return True
        return False

countVotesLock = threading.Lock()
countUserVotesLock = threading.Lock()

@app.route("/countVotes")
def countVotes():
    res = "busy"
    if not countVotesLock.locked():
        countVotesLock.acquire()
        thread = MapReduce("vote_count", "vote_cache")
        res = "done" if thread.run() else "busy"
        countVotesLock.release()
    return jsonify({"calculation": res})

@app.route("/countUserVotes")
def countUserVotes():
    res = "busy"
    if not countUserVotesLock.locked():
        countUserVotesLock.acquire()
        thread = MapReduce("user_vote_count", "user_votes_cache")
        res = "done" if thread.run() else "busy"
        countUserVotesLock.release()
    return jsonify({"calculation": res})

if __name__ == '__main__':
    updater = Updater()
    updater.start()
    app.run(host='0.0.0.0',port=8080)
    running = False
    updater.join()
