package com.sweet.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sweet.helper.DBHelper;

public class CnrgsCreateIndustry {

	public static void main(String[] args) {
		long s=System.currentTimeMillis();
		// TODO Auto-generated method stub
		String srcFile="E:/SWEET/砼网/文档/行业基础数据.txt";
		String toFile="E:/SWEET/砼网/文档/行业基础数据--处理后.txt";
//		readFileByLines(srcFile,toFile);
		
//		System.out.println(isNumeric("0"));
//		System.out.println((System.currentTimeMillis()-s)+"ms");
		String fileName="E:/oldCode.txt";
		String rename="E:/oldCode22222.txt";
		
		System.out.println(rename( fileName, rename));
	}
	 public static void readFileByLines(String srcFile,String toFile) {
		 	
	    	File file = new File(srcFile);
	        BufferedReader reader = null;
//	        StringBuffer sb=new StringBuffer();
	        System.out.println("文件："+srcFile);
	        boolean flg=false;
	        DBHelper dbHelper=new DBHelper();
	        
	        int jobIndustryId=0,jobCategoryId=0;
	        try {
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = null;
	            int i=0;
	            String start="01";
	            String end="";
	            String jobName="";
	            while ((tempString = reader.readLine()) != null) {
	            	//将占位符替换为pageid
//	            	if(tempString.indexOf("zhaosj")>-1){
//	            		tempString=tempString.replace("zhaosj", "");
//	            	}
	            	if(tempString.length()==0){
	            		continue;
	            	}
	            	end=tempString.substring(0,2);
	            	if(end.startsWith(start)){
	            		i=0;
	            		if(end.equals(start)){
	            			String temend=tempString.substring(0,4);
	            			if(temend.equals(start)){
	            				start=temend;
		            			jobName=(tempString);
			            	}else{
			            		if(isNumeric(temend)){
			            			start=temend;
			            		}
			            		jobName=(tempString);
			            	}
//	            			System.out.println(tempString);
		            	}else{
		            		jobName=(end+"="+tempString);
		            	}
	            	}else{
	            		end=tempString.substring(0,2);
	            		if(isNumeric(end)){
	            			start=end;
	            			if(end.startsWith(start)){
	            				i=0;
	    	            		if(end.equals(start)){
	    	            			String temend=tempString.substring(0,4);
	    	            			if(temend.equals(start)){
	    	            				start=temend;
	    		            			jobName=(tempString);
	    			            	}else{
	    			            		if(isNumeric(temend)){
	    			            			start=temend;
	    			            		}
	    			            		jobName=(tempString);
	    			            	}
//	    	            			System.out.println(tempString);
	    		            	}else{
	    		            		jobName=(end+"="+tempString);
	    		            	}
	    	            	}
	            		}else{
	            			i++;
		            		String s="";
		            		if(i<10){
		            			s="0"+i;
		            		}else{
		            			s=""+i;
		            		}
		            		end=start+s;
		            		
		            		jobName=(end+"="+tempString);
	            		}
	            	}
	            	/*********生成html 开始************/
//	            	System.out.println(jobName);
	            	String[] name=jobName.split("=");
	            	String html="";
					if(name[0].length()==2){
						flg=true;
						//生产jsp文件
	            		html="\t\t\t</dd>\n\t\t</dl>\n\t</div>\n</li>\n<li class=\"list-group-item\">\n\t"+
	            				"<div class=\"lslia\">";
	            		String sql="INSERT INTO `rgs_job_industry`(`identifier`,`name`,`recorder`,`recordTime`,`curStatus`)"+
									"VALUES"+
									"('"+
									name[0]+"','"+
									name[1]+"',"+
									"'admin',"+
									"'2017-01-08 13:40:00',"+
									"'2')";
	            		jobIndustryId++;
	            		dbHelper.executeSql(sql);
					}else if(name[0].length()==4){
						if(flg){
							html="\t\t<dl class=\"hide-list\">\n\t\t\t<dt>\n"+
									"\t\t\t\t<a href=\"javascript:;\">"+
										name[1]+
									"</a>"+
								"\n\t\t\t</dt>\n\t\t\t<dd>";
						}else{
							html="\t\t\t</dd>\n\t\t</dl>\n\t\t<dl class=\"hide-list\">\n\t\t\t<dt>\n"+
									"\t\t\t\t<a href=\"javascript:;\">"+
										name[1]+
									"</a>"+
								"\n\t\t\t</dt>\n\t\t\t<dd>";
						}
						flg=false;
						jobCategoryId++;
						
						String sql="INSERT INTO `rgs_job_category`"+
								"(`jobIndustryId`,`identifier`,`name`,`recorder`,`recordTime`,`curStatus`)VALUES"+
								"("+
								jobIndustryId+",'"+
								name[0]+"','"+
								name[1]+"',"+
								"'admin',"+
								"'2017-01-08 13:40:00',"+
								"'2')";
						System.out.println(sql);
            		dbHelper.executeSql(sql);
					//	System.out.println("fun_a['"+name[0]+"']='"+name[1]+"';");
	            	}else if(name[0].length()==6){
	            		html="\t\t\t\t<a href=\"javascript:;\">"+name[1]+"</a>";
//	            		System.out.println("fun_a['"+name[0]+"']='"+name[1]+"';");
	            		String sql="INSERT INTO `rgs_job_title`(`jobIndustryId`,`jobCategoryId`,`identifier`,`name`,`recorder`,`recordTime`,`curStatus`)VALUES("+
								jobIndustryId+","+
								jobCategoryId+",'"+
								name[0]+"','"+
								name[1]+"',"+
								"'admin',"+
								"'2017-01-08 13:40:00',"+
								"'2')";
            		dbHelper.executeSql(sql);
	            	}
					
				//	appendMethodB("C:/Users/sweet/Documents/jobName.html",html+"\n");
					
					/*********生成html 结束************/
	            	//生产jsp文件
//	            	appendMethodB(toFile,jobName+"\n");
	            }
//				System.out.println("all：\n"+sb.toString());
	            reader.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (reader != null) {
	                try {
	                    reader.close();
	                } catch (IOException e1) {
	                }
	            }
	        }
	    }
	 public static boolean isNumeric(String str){ 
	   Pattern pattern = Pattern.compile("[0-9]*"); 
	   Matcher isNum = pattern.matcher(str);
	   if( !isNum.matches() ){
	       return false; 
	   } 
	   return true; 
	}
	 /**
	  * B方法追加文件：使用FileWriter
	  * @param fileName 文件名
	  * @param content 追加内容
	  */
    public static void appendMethodB(String fileName, String content) {
    	File file =new File(fileName);
    	File parent =file.getParentFile();
    	//文件夹不存在，则创建文件夹
    	if(!parent.exists()){
    		parent.mkdirs();
    	}
        try {
        	//文件不存在，则创建文件
        	if(!file.exists()){
        		System.out.println("文件不存在");
        		file.createNewFile();
        		System.out.println("创建成功");
        	}
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 文件重命名
     * @param fileName
     * @param rename
     * @return
     */
    public static boolean rename(String fileName,String rename){
    	File file=new File(fileName);
    	if(file.exists()){
    		return file.renameTo(new File(rename));
    	}
    	return false;
    }
    
}