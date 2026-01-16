package data_algorithm.chapter_8;

import data_algorithm.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PubFriendJobDriver extends Configured implements Tool  {
    @Override
    public int run(String[] args) throws Exception {
        HdfsUtils.deletePath(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        Path inpath = new Path(args[0]);
        Path outpath = new Path(args[1]);

        job.setJarByClass(PubFriendJobDriver.class);
        job.setMapperClass(PubFriendMapper.class);
        job.setReducerClass(PublicFriendReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, inpath);
        FileOutputFormat.setOutputPath(job,outpath);

        int status = job.waitForCompletion(true)?0:1;
        return status;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("args exception");
            System.exit(1);
        }
        ToolRunner.run(new PubFriendJobDriver(), args);
    }
}