package com.net.start;
/*
 * Author:huaxiaziyun
 * Create data :2015.9.11
 * Description:过滤评论中小广告
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class zheze {
	public static void main(String args[]) throws IOException{
		String Comment_Txt=readFile("E:/text.txt");//读取用户评论
		String Phrase_Txt=readFile("E:/phrase.txt");//读取用户评论
		String Rst="";
		String[] s = Phrase_Txt.split("，");
		for(int i=0;i<s.length;i++){
			char[] Sm=s[i].toCharArray();
			for(int j=0;j<Sm.length;j++){
				String m="["+Sm[j]+"]";
				Rst=Rst+m;
			}
			String regEx="[`~!@#$%^&*()+=|{}:;\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？a-zA-Z 0-9]";
			Matcher m_data=ConverCompile(Comment_Txt.toString().trim(), regEx);
			String result =m_data.replaceAll("").trim();
			Matcher n_data=ConverCompile(result, ""+Rst+"");
			 while(n_data.find()){ 
				 System.out.println(n_data.group());
				}
			 Rst="";
			 }
		}
	
	
/*
  * 正则匹配
  */
	private static Matcher ConverCompile(String result,String regEx){
		Pattern c = Pattern.compile(regEx);
		 Matcher mc=c.matcher(result);
		return mc;
}
/*
  * 读取txt文件中的数据
  */
	private static String readFile(String road) throws IOException {
		File file= new File(road);
		String encoding="GBK";
		 String lineTXT = null; 
		 if(file.isFile()&&file.exists()){
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
				BufferedReader bufferedReader=new BufferedReader(read);
				lineTXT=bufferedReader.readLine();
				read.close();
		 }
		         return lineTXT;
	
                
                   }
}