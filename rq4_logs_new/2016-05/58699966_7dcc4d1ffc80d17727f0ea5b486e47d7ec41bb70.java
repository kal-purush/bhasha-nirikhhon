import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AffineGap {

	private static String Y = "$ABCD", X = "$ACCD";
	private static int t = 1;

	public static class AffineGapMapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private Text indices = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()){
				int i = Integer.parseInt(itr.nextToken());
				int j = Integer.parseInt(itr.nextToken());
				int weight = Integer.parseInt(itr.nextToken());
				indices.set(i+" " + j);				
				context.write(indices,new IntWritable(weight));
				if (i == 0){
					for(int row = 1;row<X.length();row++){
						indices.set(row+ " " + j);						
						//System.out.println("I : " + row + " J: " + j + "W : " + weight + " wIns : " + wIns(0,row));
						context.write(indices,new IntWritable(weight+wIns(0, row)));
						
					}		
					int k = j+1;
					if(k < X.length()){			
						indices.set(1 + " " + k );
						System.out.println("I : 1 J : " + k + " W: " + weight + " CHARS: " + X.charAt(1) + Y.charAt(k));
						context.write(indices,new IntWritable(weight+s(X.charAt(1),Y.charAt(k))));
					}
				}
				else if (i !=0 && j == 0){
					for(int col = 1;col<Y.length();col++){
						indices.set(i+" " + col);
						//System.out.println("I : " + i + " J: " + col + "W : " + weight + " wDel : " + wDel(0, col));
						context.write(indices, new IntWritable(weight+wDel(0, col)));
					}
					int k = i+1;
					if(k < Y.length()){
						indices.set(k+" "+1);
						System.out.println("I : " +k +" J : 1  W: " + weight + " S: " + X.charAt(k) + Y.charAt(1));
						context.write(indices,new IntWritable(weight+s(X.charAt(k),Y.charAt(1))));
					}
				}
			}
		}
	}


	public static class AffineGapMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		
		private Text indices = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			
			while (itr.hasMoreTokens()){
				int i = Integer.parseInt(itr.nextToken());
				int j = Integer.parseInt(itr.nextToken());
				int weight = Integer.parseInt(itr.nextToken());
				indices.set(i+" " + j);				
				context.write(indices,new IntWritable(weight));
				System.out.println("I: "+i+" J: "+j+" W:"+weight + "MAX: " + Math.max(1, t+1-X.length()) + " MIN : "+Math.min(t, X.length()) + " JCOND : " + (t+1-i));
				if( i >= Math.max(1, t+1-X.length())){
					System.out.println("I MAX succeeded");
				}
				if( i <= Math.min(t, X.length())){
					System.out.println("I MIN succeeded");
				}
				if (j == t+1-i){
					System.out.println("j COND succeeded");
				}
				if(i >= Math.max(1, t+1-X.length()) && i <= Math.min(t, X.length()) && j == t+1-i){
					System.out.println("Condition success");
					if(((i+1) < X.length()) && ((j+1) < Y.length())){
						int k = i + 1;
						int l = j + 1;
						indices.set(k+" "+l);
                                                System.out.println("I : " + k +" J : "+ l +"  W: " + weight + " S: " + X.charAt(k) + Y.charAt(l));
                                                context.write(indices,new IntWritable(weight + s(X.charAt(k),Y.charAt(l))));

					}
					
					for(int q = j+1; q<Y.length(); q++){
						indices.set(i+" "+q);
						System.out.println("I : " + i + " J : " + q + " W: " + weight + " Ins: " + wIns(j,q));
						context.write(indices, new IntWritable(weight + wIns(j, q)));
					}
					
					for(int p = i+1; p<X.length(); p++){
						indices.set(p+" "+j);
						System.out.println("I : " + p + " J : " + j + " W: " + weight + " Ins: " + wIns(i,p));
						context.write(indices, new IntWritable(weight + wIns(i, p)));
					}
				}
			}
		
	}
 }
		public static class AffineGapReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			private IntWritable result = new IntWritable();
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				int min = Integer.MAX_VALUE;			
				for(IntWritable val: values){
					min = Math.min(min, val.get());					
				}
				result.set(min);
				context.write(key, result);				
			}

		}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int M = X.length(), N = Y.length();
		String OUTPUT_PATH = "/home/hduser/hadoop-2.7.2/hw4/Input.txt";		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(OUTPUT_PATH)));
		// ROW COL X[ROW] Y[COL] G[ROW][COL]
		bw.write("0 0 0");
		bw.newLine();
		for (int j = 1; j < M; j++) {
			int value = wDel(0, j);
			bw.write("0 " + j + " " + value);
			bw.newLine();
		}
		for (int i = 1; i < N; i++) {
			int value = wIns(0, i);
			bw.write(i + " 0 " + value);
			bw.newLine();
		}
		bw.close();

		Configuration conf = new Configuration();
		FileSystem dfs = FileSystem.get(conf);
		Path outFile = new Path("/hw4Input/Input.txt");
		FSDataOutputStream out = dfs.create(outFile);
		FileInputStream in = new FileInputStream(new File(OUTPUT_PATH));
		byte[] buffer = new byte[1024];
		
		int bytesRead;
		while((bytesRead = in.read(buffer)) > 0){
			out.write(buffer, 0, bytesRead);
		}
		
		in.close();
		out.close();
		conf = new Configuration();
		Job job = Job.getInstance(conf, "AffineGap");
		job.setJarByClass(AffineGap.class);
		job.setMapperClass(AffineGapMapper1.class);
		job.setCombinerClass(AffineGapReducer.class);
		job.setReducerClass(AffineGapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/hw4Input/"));
		FileOutputFormat.setOutputPath(job, new Path("/hw4Output/0"));
		job.waitForCompletion(true);
		
		for(t = 1; t <= X.length() + Y.length() -3;t++){
			System.out.println("T :: " + t);
			conf = new Configuration();
			job = Job.getInstance(conf, "AffineGap"+t);
			job.setJarByClass(AffineGap.class);
			job.setMapperClass(AffineGapMapper2.class);
			job.setCombinerClass(AffineGapReducer.class);
			job.setReducerClass(AffineGapReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			int dir = t - 1;
			FileInputFormat.addInputPath(job, new Path("/hw4Output/"+ dir ));
			FileOutputFormat.setOutputPath(job, new Path("/hw4Output/"+ t));
			job.waitForCompletion(true);
		}

	}

	private static int wDel(int i, int j) {
		return ((j * j * j) - (i * i * i));
	}

	private static int wIns(int i, int j) {
		return ((j * j) - (i * i));
	}

	private static int s(char x, char y) {
		if (x == y) {
			return 0;
		}
		return 1;
	}

}