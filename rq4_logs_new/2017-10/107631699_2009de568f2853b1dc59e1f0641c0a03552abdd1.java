package cn.edu.nju.cs.itrace4.demo.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class RemoveBlankLine {
	
	public void processTXTFile(String basePath) {
		File dir = new File(basePath);
		if(dir.isDirectory()) {
			File[] childs = dir.listFiles();
			for(File child:childs) {
				if(child.getName().endsWith(".txt")) {
					try {
						removeLine(child.getPath());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public void removeLine(String path) throws IOException {
		File file = new File(path);
		String name = file.getName();
		String newFileName = name.substring(0,name.lastIndexOf('.'))+".log";
		List<String> contents = new LinkedList<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		while((line=br.readLine())!=null) {
			if(line.length()!=0) {
				contents.add(line);
			}
		}
		br.close();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(newFileName)));
		for(String str:contents) {
			bw.write(str);
			bw.newLine();
		}
		bw.close();
	}
	
	public static void main(String[] args) {
		RemoveBlankLine tool = new RemoveBlankLine();
		String path = ".";
		tool.processTXTFile(path);
	}
}