import org.apache.hadoop.util.ToolRunner;

public class MapReduce {
    public static void main(String[] args) throws Exception {
        if(args[0].equals("vote_count"))
            ToolRunner.run(new VoteCounter(args), args);
        else if(args[0].equals("user_vote_count"))
            ToolRunner.run(new UserVoteCounter(args), args);
    }
}
