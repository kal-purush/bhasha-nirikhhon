package com.yang;

import java.util.HashMap;
import java.util.Map;

public class SamePart {
	public static Map<String,String>  getInfo(String req,String res){
		Map<String,String> map = new HashMap<String,String>();
		map.put("req", req);
		map.put("res", res);
		map.remove(res);
		System.out.println("testCommit");
		return map;
		
	}
}