package cn.edu.nju.cs.itrace4.exp.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.edu.nju.cs.itrace4.relation.RelationInfo;

public class LookForBug {
	
	public static Set<String> getClassFromRI(RelationInfo ri){
		Set<String> set = new HashSet<String>();
		Map<Integer,String> map = ri.getVertexIdNameMap();
		for(int id:map.keySet()) {
			set.add(map.get(id));
		}
		return set;
	}
	
	public Set<String> getClassFromRTM(String rtmPath) throws IOException {
		Set<String> set = new HashSet<String>();
		File rtmFile = new File(rtmPath);
		BufferedReader br = new BufferedReader(new FileReader(rtmFile));
		String str = null;
		while((str=br.readLine())!=null) {
			if(str.length()!=0) {
				set.add(str.split("\\s+")[0].trim());
			}
		}
		br.close();
		return set;
	}
	
	public void process(String codePath,String rtmPath) throws IOException {
		Set<String> rtmClassSet = getClassFromRTM(rtmPath);
 		File dir = new File(codePath);
		for(File file:dir.listFiles()) {
			String fileName = file.getName().substring(0,file.getName().lastIndexOf('.'));
			if(rtmClassSet.contains(fileName)) {
				rtmClassSet.remove(fileName);
			}
		}
		System.out.println(rtmClassSet.size());
	}
	
	public static void main(String[] args) throws IOException {
		LookForBug tool = new LookForBug();
		String codePath = "data/exp/iTrust/class/code";
		String rtmPath = "data/exp/iTrust/rtm/RTM_CLASS.txt";
		tool.process(codePath, rtmPath);
	}
}