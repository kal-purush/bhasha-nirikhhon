import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Bigram{

  public static class BigramMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private Text bigram = new Text();

private final static IntWritable one = new IntWritable(1);
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String previous = null;
     
            
      while (itr.hasMoreTokens()) {
    	  
    	  String current = itr.nextToken();
    	  if(previous != null){
            bigram.set(previous+" "+current);
            context.write(bigram, one);
 
   	  }
    	  
    	  previous = current;
      }
    }
  }
  
  public static class BigramReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      result.set(sum);
      context.write(key, result);
      

    }
    
    
    protected void cleanup(Context context) throws IOException,
    InterruptedException {
     }
  }
  
   private static List<BigramPair> generateOutput(Path output, Configuration conf) throws IOException{
	   
	   List<BigramPair> bigrams = new ArrayList<BigramPair>();
	   FileSystem fs = output.getFileSystem(conf);
	   FileStatus fileStatus = fs.getFileStatus(output);
	
	   
	   if(fileStatus.isDir()){
		   
		FileStatus[] listStatus = fs.listStatus(output, new PathFilter(){

			@Override
			public boolean accept(Path path) {
				return !path.getName().startsWith("_");
			}
		});
		
		for(FileStatus f : listStatus){
			bigrams.addAll(generateOutput(f.getPath(), conf));
			return bigrams;
		   }
	   }
	   
	   else
	   {
		   BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(output)));
			String line;
			line=br.readLine();
			while (line != null){
					String[] token = line.split("\\s+");
					
					BigramPair p = new BigramPair(token[0],token[1],Integer.parseInt(token[2]));
					bigrams.add(p);
					line=br.readLine();
	       }   
		     
	   }
	return bigrams;
	     
   }
   
   
   
   
   

  public static void main(String[] args) throws Exception {

    String output = args[1];
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: Bigram count <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Bigram count");
    job.setJarByClass(Bigram.class);
    job.setMapperClass(BigramMapper.class);
    job.setCombinerClass(BigramReducer.class);
    job.setReducerClass(BigramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(output));
    
    
    job.waitForCompletion(true);
    
    List<BigramPair> bigrams = new ArrayList<BigramPair>();
    
    try{
    	bigrams = generateOutput(new Path(output), conf);
    }
    catch(Exception e){
    	e.printStackTrace();
    }
    
    Collections.sort(bigrams, new Comparator<BigramPair>(){

		@Override
		public int compare(BigramPair o1, BigramPair o2) {
			// TODO Auto-generated method stub
		
			return o1.compareTo(o2);
		}
    });
    
    System.out.println("\n\n----------Generating the sorted Histogram---------");
     System.out.println(" Pair, Frequency");
    //finding the total bigrams count
    int sum = 0;
    for(BigramPair b: bigrams){
    	sum += b.getFrequency().get();
    	System.out.println("<"+b.getFirst()+" , "+b.getSecond()+">  , "+b.getFrequency());
    }
    
    //The 10% of total woudl be
    System.out.println("The total number of bigrams count is "+ sum);
    
    int requiredCount = sum/10;
    int countBigrams =0;
    

   System.out.println("\n\nThe bigrams required to meet 10% of total are");
    System.out.println("Pair, Frequency");
    //print the top-N to meet the required count
    for(BigramPair b: bigrams){
    	if(requiredCount>=0){
    	requiredCount = requiredCount - b.getFrequency().get();
    	System.out.println("<"+ b.getFirst()+" , "+b.getSecond()+">  , "+b.getFrequency());
    	countBigrams++;
    	}
    	else{
    		break;
    	}
    	
    }
    
    System.out.println("\n\nCount of top-N bigrams to meet 10% of total are "+ countBigrams);
    
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}