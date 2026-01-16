import java.util.HashMap;
import java.util.Map;

public class Main {

	public static void main(String[] args) {
		
		Map <String, String> map = new HashMap<String,String>();
		map.put("a", "Hello, ");
		map.put("b", "World!");
		map.put("c", "!!!");
		
		System.out.println(map);
		System.out.println(addNewKeyToMap(map, "a", "c"));
		

	}
	
	public static Map<String, String> addNewKeyToMap(Map<String,String> map, String a, String b){
		
		if(map.containsKey(a) && map.containsKey(b)){
			
			map.put(a+b, map.get(a) + map.get(b));
		};
		
		return map;
	}

}