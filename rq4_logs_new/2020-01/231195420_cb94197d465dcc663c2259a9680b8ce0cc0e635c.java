package com.lys;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
* 简单的对日志文件进行清理（采用mapreduce）
*
*
* 1、原日志文件：hdfs://192.168.80.103:8020/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=19/2015082819
* 2、处理后的日志文件路径：hdfs://192.168.80.103:8020/web_etl/
* */
public class Main extends Configured implements Tool {
    public static void main(String[] arg) throws Exception {
        if (arg.length != 2) {
            arg = new String[]{"hdfs://192.168.80.103:8020/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=19/2015082819", "hdfs://192.168.80.103:8020/web_etl/"};

        }
        Main obj = new Main();
        int status = ToolRunner.run(obj.getConf(), obj, arg);
        System.out.println(status);

    }

    public int run(String[] arg) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);

        Path inputPath = new Path(arg[0]);
        FileInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);


        Path outPutPath = new Path(arg[1]);
        FileOutputFormat.setOutputPath(job, outPutPath);

        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        boolean succ = job.waitForCompletion(true);

        return succ ? 0 : 1;

    }
}