package Dec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class BJ1302_베스트셀러 {
	static class Book implements Comparable<Book>{
		String name;
		int count;
		
		public Book(String name, int count) {
			this.name = name;
			this.count = count;
		}
		
		public int compareTo(Book o) {
			if(o.count == this.count) return this.name.compareTo(o.name);
			return o.count - this.count;
		}
	}
	public static void main(String[] args) throws NumberFormatException, IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
		int n = Integer.parseInt(bf.readLine());
		
		TreeSet<Book> books = new TreeSet<Book>();
		
		Map<String, Integer> b = new HashMap<String, Integer>();
		for (int i = 0; i < n; i++) {
			String name = bf.readLine();
			
			if(b.containsKey(name)) {
				b.put(name, b.get(name)+1);
			} else {
				b.put(name, 1);
			}
			
		}
		
		for (String name : b.keySet()) {
			books.add(new Book(name, b.get(name)));
		}
		System.out.println(books.first().name);
		
		
		

	}

}