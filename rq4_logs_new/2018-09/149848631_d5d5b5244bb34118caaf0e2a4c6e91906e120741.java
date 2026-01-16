package com.griddynamics.job;

import com.griddynamics.customtype.CompositeGroupKey;
import com.griddynamics.customtype.TimestampWritableComparable;
import com.griddynamics.mapper.ParquetFileMapper;
import com.griddynamics.mapper.UserSessionInfoMapper;
import com.griddynamics.reducer.SessionDataFlattener;
import com.griddynamics.reducer.SessionDurationReducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

public class UserSessionLength {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: usersessionlength <in> <out for text file> <out for parquet file>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "find longest session by user");
		job1.setJarByClass(UserSessionLength.class);

		job1.setMapperClass(UserSessionInfoMapper.class);
		job1.setMapOutputKeyClass(CompositeGroupKey.class);
		job1.setMapOutputValueClass(TimestampWritableComparable.class);

		job1.setCombinerClass(SessionDataFlattener.class);
		job1.setReducerClass(SessionDurationReducer.class);

		Path outputPath1=new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		job1.waitForCompletion(true);

		// Parquet conversion
		Configuration conf2=new Configuration();
		Job job2 = new Job(conf2, "write results as parquet");
		job2.setJarByClass(ParquetFileMapper.class);
		job2.setMapperClass(ParquetFileMapper.class);
		job2.setMapOutputKeyClass(GenericRecord.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Void.class);
		job2.setOutputValueClass(Group.class);
		job2.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setSchema(job2, ParquetFileMapper.AVRO_SCHEMA);
		Path outputPath2=new Path(args[2]);
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}