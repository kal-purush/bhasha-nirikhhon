package com.fionera.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RelaxDriver
        extends Configured
        implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        String[] otherArgs = new GenericOptionsParser(configuration, strings).getRemainingArgs();

        if (otherArgs == null || otherArgs.length < 2) {
            return 2;
        }

        Job job = Job.getInstance(configuration, "Relax");
        job.setJarByClass(RelaxDriver.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(RelaxMapper.class);
        job.setReducerClass(RelaxReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RelaxDriver(), args);
        System.exit(res);
    }
}