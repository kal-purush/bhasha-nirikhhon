import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Main {

	public static void main(String[] args) {
		
		ArrayList<String> list = new ArrayList<>();
		Collections.addAll(list, "a","b","c","a","s","c","d");
		System.out.println(list);
		System.out.println(returnMapKeys(list) );

	}
	
	public static Map<String, Boolean> returnMapKeys(ArrayList<String> list){
		
		Map<String, Boolean> res = new HashMap <String, Boolean>();
		int count = 0;
		
		for (String s:list) {
			count = Collections.frequency(list, s);
			if (count >1) {
				res.put(s, true);
			} else {
				res.put(s, false);
			}
			
			
		}
		
		
		return res;
		
	}

}