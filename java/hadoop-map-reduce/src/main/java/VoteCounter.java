import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.net.UnknownHostException;

public class VoteCounter extends MongoTool {

    public static class VoteMapper extends Mapper<Object, BSONObject, LongWritable, IntWritable> {

        private final static IntWritable up = new IntWritable(1);
        private final static IntWritable down = new IntWritable(-1);

        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            if(value.containsField("value") && value.containsField("songid")) {
                String vote = value.get("value").toString();
                LongWritable song_id = new LongWritable(Long.parseLong(value.get("songid").toString()));
                System.out.println(vote);
                if(vote.equals("1"))
                    context.write(song_id, up);
                else if(vote.equals("-1"))
                    context.write(song_id, down);
            }

        }
    }

    public static class VoteReducer extends Reducer<LongWritable, IntWritable, NullWritable, MongoUpdateWritable> {
        private MongoUpdateWritable reduceResult = new MongoUpdateWritable();

        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            BasicBSONObject query = new BasicBSONObject("_id", key.toString());
            BasicBSONObject update = new BasicBSONObject("$set", new BasicBSONObject("value", sum));
            reduceResult.setQuery(query);
            reduceResult.setModifiers(update);

            context.write(null, reduceResult);
        }
    }

    public VoteCounter(String[] args) throws UnknownHostException {
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

        MongoConfigUtil.setMapper(getConf(), VoteMapper.class);
        MongoConfigUtil.setReducer(getConf(), VoteReducer.class);
        MongoConfigUtil.setMapperOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
        MongoConfigUtil.setOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
    }

}

