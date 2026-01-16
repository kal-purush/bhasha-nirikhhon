package gdev;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.*;

public class JsonHiveParquet {
	
	final static Logger logger = Logger.getLogger(JsonHiveParquet.class);
	
	void run(String spark_url,String data_path) {
		
		logger.info(">>> JsonHiveParquet >>>");
		
		String sparkMaster = spark_url;
		
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		
		logger.info(">>>>>>>>>>>>>>>>>> warehouseLocation=["+warehouseLocation+"] >>>");
		
		SparkConf conf = new SparkConf();
		conf.set("spark.master",sparkMaster);
		conf.set("spark.sql.crossJoin.enabled", "true");
		conf.set("hive.metastore.uris","thrift://10.242.5.88:9084");

		logger.info(">>>>>>>>>>>>>>>>>> After Conf >>>");
		
		SparkSession spark = SparkSession
		  .builder()
		  .appName("JSON Hive Parquet")
		  .config(conf) 
		  .enableHiveSupport()
		  .getOrCreate();

		logger.info(">>>>>>>>>>>>>>>>>> After Spark session >>>");
		
		/*
		 spark.sql("CREATE TABLE IF NOT EXISTS src(json string) USING hive");
		 spark.sql("LOAD DATA INPATH 'C:\\spark_data\\events.json' INTO TABLE src");
		 spark.sql("SELECT * FROM src").show();
		 */
		
		spark.sql("SELECT * FROM default.d_bkk_dist").show();
		
		//spark.sql("select * from ext_tabs.stocks").show();

		logger.info(">>>>>>>>>>>>>>>>>> After Select HPT >>>");

		//spark.sql("CREATE TABLE src (key INT, value STRING) USING hive");
		//spark.sql("LOAD DATA LOCAL INPATH 'C:\\spark_data\\events.json' INTO TABLE src");
		
		//"_t":1480647647,"_p":"rattenbt@test.com","_n":"app_loaded","device_type":"desktop"
		//"_t":1505755829,"_p":"ijackson@test.com","_n":"added_to_team","account":"1234"
	
	}

}