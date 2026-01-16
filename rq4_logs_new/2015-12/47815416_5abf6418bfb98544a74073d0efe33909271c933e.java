package com.nowcoder.course;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Lesson1 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Job job1 = Job.getInstance(getConf(), "step1");
		job1.setJarByClass(Lesson1.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/count"));
		job1.waitForCompletion(true);
		Job job2 = Job.getInstance(getConf(), "step2");
		job2.setJarByClass(Lesson1.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/intersection"));

		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(getConf(), "step3");
		job3.setJarByClass(Lesson1.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[1] + "/count"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/job3"));

		job3.waitForCompletion(true);

		Job job4 = Job.getInstance(getConf(), "step4");
		job4.setJarByClass(Lesson1.class);
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(args[1] + "/intersection"));
		FileInputFormat.addInputPath(job4, new Path(args[1] + "/job3"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/job4"));

		job4.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Lesson1(), args);
		System.exit(res);
	}

}