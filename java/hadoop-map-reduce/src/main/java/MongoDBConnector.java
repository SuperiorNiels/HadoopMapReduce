import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.net.UnknownHostException;

public class MongoDBConnector extends MongoTool {

    public MongoDBConnector(String[] args) throws UnknownHostException {
        setConf(new Configuration());

        if (MongoTool.isMapRedV1()) {
            MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
            MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
        }

        MongoConfigUtil.setInputURI(getConf(), "mongodb://" + args[0] + ":" + args[1] + "/" + args[2]);
        MongoConfigUtil.setOutputURI(getConf(), "mongodb://" + args[0] + ":" + args[1] + "/" + args[3]);

        MongoConfigUtil.setMapper(getConf(), VoteCounter.VoteMapper.class);
        MongoConfigUtil.setReducer(getConf(), VoteCounter.VoteReducer.class);
        MongoConfigUtil.setMapperOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
        MongoConfigUtil.setOutputKey(getConf(), LongWritable.class);
        MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MongoDBConnector(args), args);
    }

}
