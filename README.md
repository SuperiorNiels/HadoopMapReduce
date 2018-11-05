# HadoopMapReduce

## Hadoop

!!! Set HADOOP_HOME and JAVA_HOME environment variables !!!

## Python Flask API

Dependencies:
- Flask
- requests

> \# pip install flask requests

API:
- Calculate total votes per song: *[ip_addres]:8080/countVotes*
- Calculate total votes per user: *[ip_addres]:8080/countUserVotes*
- Calculate total votes per timestamp on a song(id): *[ip_addres]:8080/timeCount/<song_id>/<timestamp>*

