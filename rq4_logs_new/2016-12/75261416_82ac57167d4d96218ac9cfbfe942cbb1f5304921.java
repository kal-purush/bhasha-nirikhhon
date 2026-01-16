package com.datamelt.hadoop.data;

import java.io.File;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URI;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;
import com.datamelt.util.Splitter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * used to process data using mapreduce. contains classes for mapping
 * and reducing.
 * 
 * for filtering data records, the business rule engine JaRE is used. Rules
 * are stored externally in a zip file, so nothing is hardcoded here. Rules
 * and (complex) logic can be designed in a web application.
 * 
 * @author	uwe geercken, last update: 2016-11-29
 *
 */
public class DataProcessor
{
	private static Logger logger = Logger.getLogger(DataProcessor.class);
	
	public static final String DISTCACHE_RULES_PROJECTFILE	= "rulesprojectfile";
	public static final String RULEENGINE_LIBRARY 			= "jare0.79.jar";
	
	private static BusinessRulesEngine bre;
	
	/**
	 * mapper part to prepare data fro the reduce phase by assigning
	 * key and value pairs.
	 * 
	 * @author uwe geercken, last update: 2016-11-29
	 *
	 */
	public static class DataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
		private static String[] headerFields;
		private static String[] headerFieldTypes;
		
		/**
		 * get the project zip file for the ruleengine
		 * from the distributed cache
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			// get the definition of the header fields
    		headerFields = DataRecord.getProperty(DataRecord.PROPERTY_HEADERFIELDS).split(",");
    		logger.info("property file defines [" + headerFields.length + "] header fields");
    		// get the definition of types for each header field
    		
    		// we have as many types as we have fields
    		headerFieldTypes = new String[headerFields.length];
    		// loop over header fields and get the corresponding types
    		for(int i=0;i<headerFields.length;i++)
			{
    			// get the field type
    			headerFieldTypes[i] = DataRecord.getPropertyFieldType(headerFields[i]);
    			
    			// if the property was not found we log the error and set the
    			// type to "string" as a default
    			if(headerFieldTypes[i].equals(DataRecord.TYPE_UNDEFINED))
    			{
    				logger.error("undefined field type for field [" + headerFields[i] +"] in propertiesfile");
    				// change type to default
    				headerFieldTypes[i] = Splitter.FIELDTYPE_STRING;
    				logger.info("changed undefined field type for field [" + headerFields[i] +"] to default type [" + Splitter.FIELDTYPE_STRING + "]");
    			}
			}
    		
			URI[] files = context.getCacheFiles();
			final String distributedCacheFilename =	context.getConfiguration().get(DISTCACHE_RULES_PROJECTFILE);
			for (URI uri: files) 
			{
				File file = new File(uri.getPath());
				logger.info("found file in distributed cache [" + file.getName() + "]");
				if (file.getName().equals(distributedCacheFilename)) 
				{
					initializeRuleEngine(new ZipFile(file.getPath()));
					break;
				}
			}
		}
		
		/**
		 * initialize the static rule engine instance with a
		 * a zip file, created with the business rules maintenance tool
		 * 
		 * @param file	a zip file for the rule engine
		 */
		private void initializeRuleEngine(ZipFile file)
		{
			logger.info("Initializing JaRE Business Rule Engine v" + BusinessRulesEngine.getVersion() + " with file [" + file.getName() + "]");
			try
			{
				bre = new BusinessRulesEngine(file);
				//bre.setPreserveRuleExcecutionResults(false);
			}
			catch(Exception ex)
			{
				 logger.error(ex.getMessage());
			}
		}
		
		/**
		 * Split the data record in its individual fields, get the definition of the
		 * header fields and create a RowFieldCollection from it. This will then be
		 * passed to the rule engine and when the rule engine runs it evaluates the
		 * rules against the data.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	try
        	{
        		// split data record
        		Splitter splitter = new Splitter(Splitter.TYPE_COMMA_SEPERATED,Splitter.SEPERATOR_TAB);

        		// create a collection of the fields taking into account the header fields and the field types
        		RowFieldCollection collection = new RowFieldCollection(headerFields,splitter.getFields(value.toString(),headerFieldTypes));
        		
        		// run the Rule Engine on the collection we just created for the data record
    	    	bre.run(key.toString(), collection);
    	    	
    	    	// if the run of the rule engine has no groups which failed, write key and value to the context
    	    	if(bre.getNumberOfGroupsFailed()==0)
    	    	{
    	    		// get the field that contains the value for calculations
    	    		RowField valueField = collection.getField(DataRecord.getProperty(DataRecord.PROPERTY_CALCULATIONFIELD));
    	    		
    	    		// if no keyfields have been defined, use the key defined by the map process
    	    		String rowKey = DataRecord.getKey(collection);
    	    		if(rowKey.equals(""))
    	    		{
    	    			rowKey = key.toString();
    	    		}
    	    		// get the constructed key and value for the current row of data
    	    		context.write(new Text(rowKey), new DoubleWritable((Double)valueField.getValue()));
    	    	}
    	    	
    	    	// clear the results of the ruleengine execution before the next data arrives
    	    	bre.getRuleExecutionCollection().clear();
        	}
        	catch(Exception ex)
        	{
        		logger.error(ex.getMessage());
        	}
        }
    }

	/**
	 * reduce class. sum up values for the defined key
	 * 
	 * @author uwe geercken, last update: 2016-11-29
	 *
	 */
    public static class DataReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
    {
    	/**
    	 * reduce the keys and values from the mapping phase
    	 * 
    	 * values will be summed in the reduce phase
    	 */
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double sum = 0;
            // sum up values
            for (DoubleWritable value : values)
            {
            	sum += value.get();
            }
            // write to the context as key and double
            context.write(key, new DoubleWritable(sum));
        }
    }
    
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ";");
        
        Job job = Job.getInstance(conf, "data processor");
        
        job.setJarByClass(DataProcessor.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setCombinerClass(DataReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // we use the JaRE business rule engine for evaluating the data
        job.addArchiveToClassPath(new Path(RULEENGINE_LIBRARY));
        
        // the ruleengine project file - put it in the distributed cache
        Path rulesFile = new Path(args[2]);
        job.addCacheFile(rulesFile.toUri());
        job.getConfiguration().set(DISTCACHE_RULES_PROJECTFILE, rulesFile.getName());
        
        // input and output path of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);

    }
}