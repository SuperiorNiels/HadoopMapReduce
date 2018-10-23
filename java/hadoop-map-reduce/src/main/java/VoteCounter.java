import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.bson.BSONObject;

public class VoteCounter {

    public static class VoteMapper extends Mapper<Object, BSONObject, LongWritable, IntWritable>{

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

    public static class VoteReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}