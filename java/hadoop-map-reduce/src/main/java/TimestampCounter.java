import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampCounter extends MongoTool {

    public static String startSong;
    public static long songId;

    public static class timestampMapper extends Mapper<Object, BSONObject, IntWritable, IntWritable> {
        /*
         * startSong is in ISO 8601 format
         * Create a map that keeps track of timesslots and votes
         * so the actual structure will be <songid, <timeslot, vote>>
         * in the reducejob we will count the votes per slot
         */
        @Override
        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if(value.containsField("timestamp") && value.containsField("songid") && value.containsField("value")) {
                String timestamp = value.get("timestamp").toString();
                long song_id = Long.parseLong(value.get("songid").toString());
                // if the song id is equal to the song id that we want, map it
                if( song_id == songId) {
                    IntWritable vote = new IntWritable(Integer.parseInt(value.get("value").toString()));
                    // assign each vote to the correct timeslot using the timestamp(each timeslot is 10sec)

                    //first there is a need to convert from ISO 8601 to amount of seconds so we can compute the times
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
                    int timeslot = (int) Math.floor((tempTime-startTime)/10000);
                    System.out.println("Timeslot: " + timeslot);
                    context.write(new IntWritable(timeslot), vote);
                }
            }

        }
    }
    /*
     * computes the votes per timeslot
     */
    public static class timestampReducer extends Reducer<IntWritable, IntWritable, LongWritable, MongoUpdateWritable> {
        private MongoUpdateWritable reduceResult = new MongoUpdateWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            LongWritable longW = new LongWritable(songId);
            BasicBSONObject query = new BasicBSONObject("_id", songId);
            BasicBSONObject obj = new BasicBSONObject();
            obj.append("timeslot" + key.toString(), sum);
            BasicBSONObject update = new BasicBSONObject("$set", obj);

            reduceResult.setQuery(query);
            reduceResult.setModifiers(update);

            context.write(longW, reduceResult);
        }
    }

    public TimestampCounter(String[] args, String startSong, long songId) throws UnknownHostException {
        TimestampCounter.startSong = startSong;
        TimestampCounter.songId = songId;
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
        MongoConfigUtil.setMapperOutputKey(getConf(), IntWritable.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
        MongoConfigUtil.setOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
    }

}


