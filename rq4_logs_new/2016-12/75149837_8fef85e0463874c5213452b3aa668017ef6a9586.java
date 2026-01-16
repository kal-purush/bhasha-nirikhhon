import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profiling_DS_Accident {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Profiling_DS_Accident.class);
		job.setJobName("DataSet_Accident_Profiling");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Profiling_DS_Accident_Mapper.class);
		job.setReducerClass(Profiling_DS_Accident_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}