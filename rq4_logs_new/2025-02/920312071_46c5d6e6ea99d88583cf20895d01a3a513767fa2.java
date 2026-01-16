import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in); 
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        List<String> MaxList = new ArrayList<>();
        
        int N = sc.nextInt();
        int max = 1;
        int cnt;
        
        for(int i = 0; i < N; i++) {
        	String str = sc.next();
        	
        	if(map.containsKey(str))  
        		cnt = map.get(str) + 1;
        	else
        		cnt = 1;
        	map.put(str, cnt);
        	
        	if (cnt == max) {
            	MaxList.add(str);
            }
        	else if (cnt > max) {
        		max = cnt;
        		MaxList.clear();
        		MaxList.add(str);
        	}      	
        }
        sc.close();
       
        Collections.sort(MaxList);
        System.out.println(MaxList.get(0));
    }       
}