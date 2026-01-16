package HelloWorld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XMLDriver extends Configured implements Tool{
	private static final String INPUT_PATH = "/user/2288733m/team.xml";

    //Run via: java XML.HelloWorld.XMLDriver /full/file/path/to/projectstats.xml projectName numberWhitelistedProjects

	public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());
			conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
		conf.set("XMLConvert.jar", "file:///users/msc/2288733m/IdeaProjects/XML_Jar/XMLConvert.jar"); //Only neccessary for eclipse?
			//Setting vars for start and end of each XML entry
		    conf.set("xmlinput.start", "<user>");
		    conf.set("xmlinput.end", "</user>");
            conf.set("numWhitelistedProjects", args[2]); //Grabbing the number of whitelisted projects from 2nd argument!

			Job job = Job.getInstance(conf);

            job.setJobName(args[1] + "_MagCalc_" + Time.now());
			job.setJarByClass(XMLDriver.class);

            //Setting class types
            job.setInputFormatClass(XMLInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

		    job.setMapperClass(XMLMapper.class);
		    job.setReducerClass(xmlreducer.class);

		    //Setting the output type
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);

            //User can set input and output paths via arguments
            FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
            FileOutputFormat.setOutputPath(job, new Path(job.getJobName() + "_output"));

            //There can be only one, highlander!
            job.setNumReduceTasks(1);

            //Gogogo!
            job.submit();

		    return (job.waitForCompletion(true) ? 0 : -1);
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new XMLDriver(),args);
		System.exit(exitCode);
	}
	

}