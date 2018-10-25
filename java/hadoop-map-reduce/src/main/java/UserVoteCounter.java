import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.net.UnknownHostException;

public class UserVoteCounter extends MongoTool {

    public static class UserVoteMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        private final Text keyText;
        private final IntWritable intWritable = new IntWritable(1);

        public UserVoteMapper() {
            super();
            this.keyText = new Text();
        }

        @Override
        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            keyText.set(value.get("uid").toString());
            context.write(keyText, intWritable);
        }

    }

    public static class UserVoteReducer extends Reducer<Text, IntWritable, NullWritable, MongoUpdateWritable> {

        private MongoUpdateWritable reduceResult;

        public UserVoteReducer() {
            reduceResult = new MongoUpdateWritable();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable ignored : values)
                ++sum;

            BasicBSONObject query = new BasicBSONObject("_id", key.toString());
            BasicBSONObject update = new BasicBSONObject("$set", new BasicBSONObject("value", sum));
            reduceResult.setQuery(query);
            reduceResult.setModifiers(update);

            context.write(null, reduceResult);
        }

    }

    public UserVoteCounter(String[] args) throws UnknownHostException {
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

        MongoConfigUtil.setMapper(getConf(), UserVoteMapper.class);
        MongoConfigUtil.setReducer(getConf(), UserVoteReducer.class);
        MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
        MongoConfigUtil.setOutputKey(getConf(), Text.class);
        MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
    }


}
