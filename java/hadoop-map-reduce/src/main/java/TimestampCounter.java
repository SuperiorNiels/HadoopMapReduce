import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.bson.BSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampCounter extends MongoTool {

    public static String startSong;

    public static class timestampMapper extends Mapper<Object, BSONObject, LongWritable, MapWritable> {
        /*
         * startSong is in ISO 8109 format
         * Create a map that keeps track of timesslots and votes
         * so the actual structure will be <songid, <timeslot, vote>>
         * in the reducejob we will count the votes per slot
         */
        @Override
        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if(value.containsField("timestamp") && value.containsField("songid") && value.containsField("value")) {
                String timestamp = value.get("timestamp").toString();
                LongWritable song_id = new LongWritable(Long.parseLong(value.get("songid").toString()));
                IntWritable vote = new IntWritable(Integer.parseInt(value.get("value").toString()));
                MapWritable timeslots = new MapWritable();
                // assign each vote to the correct timeslot using the timestamp(each timeslot is 10sec)

                //first there is a need to convert from ISO 8109 to amount of seconds so we can compute the times
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                Date startD = null;
                Date timestampD = null;
                try {
                    startD = sdf.parse(startSong);
                    timestampD = sdf.parse(timestamp);
                } catch(Exception e) {
                    System.err.println("Something went wrong parsing the timestamp");
                }
                long startTime = startD.getTime();
                long tempTime = timestampD.getTime();
                int timeslot = (int) Math.floor((tempTime-startTime)/10);
                timeslots.put(new IntWritable(timeslot), vote);
                context.write(song_id, timeslots);
            }

        }
    }
    /*
     * computes the votes per timeslot for a specific song id
     */
    public static class timestampReducer extends Reducer<LongWritable, MapWritable, LongWritable, MapWritable> {
        private MapWritable result = new MapWritable();

        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            for (MapWritable val : values) {
                //this check is necessary because the default value for a map is not 0!
                if(result.containsKey(val.keySet().toArray()[0])) {
                    // if the a key already exists add the vote from the mapper function to the result
                    int temp = Integer.parseInt(result.get(result.keySet().toArray()[0]).toString());
                    int temp2 = Integer.parseInt(result.get(val.keySet().toArray()[0]).toString());
                    int store = temp + temp2;
                    result.put((Writable) val.keySet().toArray()[0], new IntWritable(store));
                } else {
                    // if the given timeslot is not in the result yet, put the vote from the mapper map into the result
                    result.put((Writable) val.keySet().toArray()[0], val.get(val.keySet().toArray()[0]));
                }
            }
            //returns a map with votes in a several timeslots of 10seconds
            context.write(key, result);
        }
    }

    public TimestampCounter(String[] args, String startSong) throws UnknownHostException {
        TimestampCounter.startSong = startSong;
        setConf(new Configuration());

        if (MongoTool.isMapRedV1()) {
            MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
            MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
        }

        MongoConfigUtil.setInputURI(getConf(), "mongodb://" + args[1] + ":" + args[2] + "/" + args[3]);
        MongoConfigUtil.setOutputURI(getConf(), "mongodb://" + args[1] + ":" + args[2] + "/" + args[4]);

        MongoConfigUtil.setMapper(getConf(), timestampMapper.class);
        MongoConfigUtil.setReducer(getConf(), timestampReducer.class);
        MongoConfigUtil.setMapperOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), MapWritable.class);
        MongoConfigUtil.setOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setOutputValue(getConf(), MapWritable.class);
    }

}


