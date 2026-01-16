import java.io.BufferedReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.io.IOException;

public class Main {
	public static StringBuilder sb =new StringBuilder();
	public static Map<String,Integer> p = new HashMap<String,Integer>();
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st; 
		
		int n = Integer.parseInt(br.readLine());
		for(int i=0 ;i< n;i++) {
			String person = br.readLine();
			p.put(person, p.getOrDefault(person, 0)+1);
		}
		for(int i=0 ;i<n-1;i++) {
			String person = br.readLine();
			if(p.containsKey(person)) {
				if(p.get(person)>1) {
					p.put(person, p.get(person)-1);
				}
				else {
					p.remove(person);
				}
			}
		}
		for(Map.Entry<String, Integer> p : p.entrySet()) {
			System.out.println(p.getKey());
		}
		
	}

}