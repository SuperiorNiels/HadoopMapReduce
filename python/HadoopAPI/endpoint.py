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
           db + "." + "OUTCOLLECTION",
           "", ""]

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
        self.argument1 = ""
        self.argument2 = ""
    def setArgument1(self, new_arg):
        self.argument1 = new_arg
    def setArgument2(self, new_arg):
        self.argument2 = new_arg
    def run(self):
        if not mutex.locked():
            mutex.acquire()
            command[4] = self.operation
            command[8] = db + "." + self.outCollection
            command[9] = self.argument1
            command[10] = self.argument2
            print("Executing: " + self.operation + ", saving to collection: " + self.outCollection)
            #print(command)
            call(command, stdout=devnull, stderr=devnull)
            mutex.release()
            return True
        return False

@app.route("/countVotes")
def countVotes():
    thread = MapReduce("vote_count", "vote_cache")
    res = "done" if thread.run() else "busy"
    return jsonify({"calculation": res})

@app.route("/countUserVotes")
def countUserVotes():
    thread = MapReduce("user_vote_count", "user_votes_cache")
    res = "done" if thread.run() else "busy"
    return jsonify({"calculation": res})

@app.route("/timeCount/<song_id>/<timestamp>")
def countTime(song_id, timestamp):
    thread = MapReduce("time_count", "time_vote_cache")
    thread.setArgument1(str(timestamp))
    thread.setArgument2(str(song_id))
    res = "done" if thread.run() else "busy"
    return jsonify({"calculation": res})

if __name__ == '__main__':
    updater = Updater()
    updater.start()
    app.run(host='0.0.0.0',port=8080)
    running = False
    updater.join()
