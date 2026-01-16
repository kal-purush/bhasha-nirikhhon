
import java.util.*;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you mapper function
        	String Line = value.toString();
        	Line.toLowerCase();
        	Line.replaceAll("[^a-z0-9]", " ");
        	String[] words = Line.split("[ \t]+");
        	ArrayList<Text> cW_set = new ArrayList<Text>();
        	ArrayList<MapWritable> qWs_set = new ArrayList<MapWritable>();
        	for(int i = 0; i < words.length; i++){
        		Text contextWord = new Text(words[i]);
        		MapWritable queryWords = new MapWritable();
        		for(int j = 0; j < words.length; j++){
        			if(i != j){
        				Text queryWord = new Text(words[j]);
        				if(queryWords.containsKey(queryWord)){
        					queryWords.replace(queryWord, new LongWritable(1 + ((LongWritable)queryWords.get(queryWord)).get()));
        				}
        				else {
        					queryWords.put(queryWord, new LongWritable(1));
        				}
        			}
        		}
        		cW_set.add(contextWord);
        		qWs_set.add(queryWords);
        	}
        	int x = 0;
        	while(x < cW_set.size()){
        		Text word = cW_set.get(x);
        		int firstIndex = cW_set.indexOf(word);
        		int lastIndex = cW_set.lastIndexOf(word);
        		if(firstIndex != lastIndex){
        			cW_set.remove(lastIndex);
        			qWs_set.remove(lastIndex);
        		}
        		else{x++;}
        	}
        	for(int i = 0; i < cW_set.size(); i++){
        		context.write(cW_set.get(i), qWs_set.get(i));
        	}
        
        	
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    /*public static class TextCombiner extends Reducer<?, ?, ?, ?> {
        public void reduce(Text key, Iterable<Tuple> tuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you combiner function
        }
    }*/

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer< Text, MapWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<MapWritable> queryMaps, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function
        	HashMap<String, Long> queryWords = new HashMap<String, Long>();
        	for(MapWritable queryMap : queryMaps){
        		for(Writable queryWord : queryMap.keySet()){
        			String word = ((Text)queryWord).toString();
        			Long count = ((LongWritable)queryMap.get(queryWord)).get();
        			if(queryWords.containsKey(word)){
        				queryWords.put(word, queryWords.get(word) + count);
        			}
        			else {queryWords.put(word, count);}
        		}
        	}
        		
        	List<String> lexicographicalQueryWords = new ArrayList<String>();
        	lexicographicalQueryWords.addAll(queryWords.keySet());
        	Collections.sort(lexicographicalQueryWords);
        	
            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            context.write(key, emptyText);
            //   Write out query words and their count
            for(String queryWord: lexicographicalQueryWords){
                String count = queryWords.get(queryWord).toString() + ">";
                String queryWordText = "<" + queryWord + ",";
                context.write(new Text(queryWordText), new Text(count));
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "EID1_EID2"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        // job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        //job.setMapOutputValueClass(?.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}