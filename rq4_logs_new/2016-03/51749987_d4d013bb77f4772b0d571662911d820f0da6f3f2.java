package middle;

import java.util.ArrayList;
import java.util.Iterator;


public class ExampleArrayList {
	public static void main(String[] args) {
		ArrayList<String> list = new ArrayList<>();

		list.add("1");
		list.add("1");
		list.add("2");
		list.add("4");
		list.add("5");
		list.add("3");

		System.out.println("1.While: ");
		int i = 0;
		while (i < list.size()) {
			System.out.println(list.get(i) + " ");
			i++;
		}

		System.out.println();

		System.out.println("2.For: ");
		for (int j = 0; j < list.size(); j++)
			System.out.print(list.get(j) + " ");

		System.out.println();

		System.out.println("3.Iterator");
		Iterator<String> itr = list.iterator();
		while (itr.hasNext())
			System.out.print(itr.next() + " ");

		System.out.println();
		
		System.out.println("4.For");
		for(String s:list)
			System.out.print(s +" ");
		
		System.out.println(); 
		
		System.out.println("5.Foreach");
		
		list.forEach((temp)->{
			System.out.print(temp + " ");
		});
		
		System.out.println();
		System.out.println("Stream");
		
		
	}

}