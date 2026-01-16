package Algorithm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//전화번호 목록
public class PhoneNumber {

	public static void main(String args[]) {
		String[] book = {"12","123","1235","567","88"};
		boolean test = true;
		Map<String,Integer> hm = new HashMap<>();
		Arrays.sort(book);
			
		for(String str : book) {
			hm.put(str, str.length());
		}
		
		
		for(int i=0; i<book.length-1; i++)
		{
			
			
			if(book[i+1].startsWith(book[i])) {
				hm.put(book[i], 1);
				test = false;
			}else {
				hm.put(book[i], 0);
			}
			
		}
		
		
		System.out.println(test);
		
		
	}
}