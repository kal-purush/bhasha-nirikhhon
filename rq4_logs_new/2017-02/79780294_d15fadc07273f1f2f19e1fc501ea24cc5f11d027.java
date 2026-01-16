package com.kevin.useranalysedemo;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import com.kevin.useranalysedemo.mr.mapper.EsCompanyMapper;
import com.kevin.useranalysedemo.mr.mapper.EsUserEventMapper;
import com.kevin.useranalysedemo.mr.mapper.EsUserEventReducer;
import com.kevin.useranalysedemo.mr.mapper.UserIntersectionMapper;
import com.kevin.useranalysedemo.mr.mapper.UserIntersectionReducer;
import com.kevin.useranalysedemo.utils.DateUtils;
import com.kevin.useranalysedemo.utils.PropertyUtil;
import com.kevin.useranalysedemo.utils.VelocityUtils;

public class UserEventAnalyzeApp {
	
	private static Logger logger = Logger.getLogger(UserEventAnalyzeApp.class); 
	
	public static void main(String[] args) throws Exception{
		
		//获取配置信息	
		String esQueryStartDate = PropertyUtil.getInstance().getString("es.query.start.date");
		String esQueryEndDate = PropertyUtil.getInstance().getString("es.query.end.date");
		String[] esQueryEventIds = PropertyUtil.getInstance().getString("es.query.event.ids").split(",");
		String esServerHost = PropertyUtil.getInstance().getString("es.server.host");
		String esServerPort = PropertyUtil.getInstance().getString("es.server.port.http");
		
		String esHadoopReadIndex = PropertyUtil.getInstance().getString("es.hadoop.read.index");
		String esHadoopReadType = PropertyUtil.getInstance().getString("es.hadoop.read.type");
		String esHadoopWriteIndex = PropertyUtil.getInstance().getString("es.hadoop.write.index");
		String esHadoopWriteType = PropertyUtil.getInstance().getString("es.hadoop.write.type");
		String esNetHttpAuthUser = PropertyUtil.getInstance().getString("es.net.http.auth.user");
		String esNetHttpAuthPass = PropertyUtil.getInstance().getString("es.net.http.auth.pass");
		String esInputJson = PropertyUtil.getInstance().getString("es.input.json");
		String hadoopServerHost = PropertyUtil.getInstance().getString("hadoop.server.host");
		String hadoopDfsPort = PropertyUtil.getInstance().getString("hadoop.dfs.port");
		String esHadoopQueryResultRootPath = PropertyUtil.getInstance().getString("es.hadoop.query.result.root.path");
		
		//获取es的查询语句		
		ArrayList<String> esQdls = new ArrayList<String>();
		for(String esQueryEventId : esQueryEventIds){
			VelocityContext velocityContext = new VelocityContext();
			velocityContext.put("esQueryStartDate", esQueryStartDate);
			velocityContext.put("esQueryEndDate", esQueryEndDate);
			velocityContext.put("esQueryEventId", esQueryEventId);
			String esQdl = VelocityUtils.getContent("velocity_vm/es_user_event_query.vm", velocityContext);
			esQdls.add(esQdl);
			logger.info(" esQdl = \n" + esQdl);
		}
		
		//通过es-hadoop 将es的查询结果导入到hadoop 中
		        
//        String nowStr = DateUtils.getNowStr(null);
//        for(int i=0; i<esQdls.size(); i++){
//        	String esQdl = esQdls.get(i);  
//        	Configuration conf = new Configuration();
//        	conf.set("mapreduce.job.jar", "UserEventAnalyzeApp.jar");  
//            conf.set("es.nodes", esServerHost+":"+esServerPort);
//            conf.set("es.resource.read", esHadoopReadIndex+"/"+esHadoopReadType);
//            conf.set("es.resource.write", esHadoopWriteIndex+"/"+esHadoopWriteType);
//            conf.set("es.net.http.auth.user",esNetHttpAuthUser);
//            conf.set("es.net.http.auth.pass",esNetHttpAuthPass);
//            conf.set("es.input.json", esInputJson);
//        	conf.set("es.query",esQdl);
//        	
//			Job job = Job.getInstance(conf,"hadoop elasticsearch");
//	        // 指定自定义的Mapper阶段的任务处理类
//	        job.setMapperClass(EsUserEventMapper.class);
//	        job.setReducerClass(EsUserEventReducer.class);
//	        // 设置map输出格式
//	        job.setOutputKeyClass(Text.class);
//	        job.setOutputValueClass(Text.class);
//	        // 设置输入格式
//	        job.setInputFormatClass(EsInputFormat.class);
//	        //job.setOutputFormatClass(EsOutputFormat.class);
//	        // 设置输出路径
//	        String hadoopPath = "hdfs://"+hadoopServerHost+":"+hadoopDfsPort+esHadoopQueryResultRootPath+"/user_event/"+nowStr+"/"+i+"/query_result";
//	        //String hadoopPath = "F:/mr-fs/"+nowStr+"/"+i+"/query_result";
//	        FileOutputFormat.setOutputPath(job, new Path(hadoopPath));
//	       
//	        // 运行MR程序
//	        job.waitForCompletion(true);	        
//		}
        
        
                
        Configuration conf = new Configuration();  
        //conf.set("mapreduce.job.jar", "UserEventAnalyzeApp.jar"); 
        Job job = Job.getInstance(conf,"hadoop intersection");
        //指定自定义的Mapper阶段的任务处理类
        job.setMapperClass(UserIntersectionMapper.class);
        job.setReducerClass(UserIntersectionReducer.class);
        //设置mr输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1);
        
        //指定要处理的原始数据所存放的路径          
        FileInputFormat.setInputPaths(job, new Path("F:/mr-fs/20170210135217/0/query_result"));  
      
        //指定处理之后的结果输出到哪个路径           
        FileOutputFormat.setOutputPath(job, new Path("F:/mr-fs/temp/out"));  
        

        boolean ret = job.waitForCompletion(true);  
        
        System.exit(ret?0:1);  
	}

}