package com.orilore.yanxue.book;
import java.io.*;
public class CharUtil2 {
	public static void mian(String[] args) throws IOException{
		try(Reader input = src;Writer output = dest){
			char[] data = new char[1024];
			int length;
			while((length = input.read(data))!=-1){
				output.write(data,0,length);
				}
			}
		}
	public static void dump(InputStream src,OutputStream dest,String charset) throws IOException{
		dump(
				new InputStreamReader(src,charset),
				new OutputStreamWriter(dest,charset),
				)
	};
	}
public static void dump(InputStream src,OutputStream dest) throws IOException{
	dump(src,dest,System.getProperty("file.encoding"));
}
}