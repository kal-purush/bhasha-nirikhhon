package Algorithm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Solution {

	
	public static void main(String args[]) {
		
		String[] book = {"12","123","12345","567","88"};
		
		
		solution(book);
		
	}
	
	
	
	
	public static boolean solution(String[] phone_book) {
		
		boolean answer = true;
		int strle = 0;
		String mapkey = "";
		Map<String,Integer> hm = new HashMap<>();
		
		for(String str : phone_book) {
			hm.put(str, str.length());
		}
	
		Iterator<Entry<String, Integer>> iter = hm.entrySet().iterator();
		
		while(iter.hasNext()){
			Entry<String, Integer> entryMap = iter.next();
			
			mapkey = entryMap.getKey();
			hm.put(mapkey, 0);
			System.out.println(hm);
		}
		
		return answer;
	}
}