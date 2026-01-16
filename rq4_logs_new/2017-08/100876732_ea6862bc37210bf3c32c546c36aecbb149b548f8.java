package intro_to_array_lists;

import java.util.ArrayList;

public class ArrayListDemo {
	public static void main(String[] args) {
		System.out.println("Here's an ArrayList of Strings:\n");
		//ArrayList of Strings
		ArrayList<String> names = new ArrayList<String>();
		
		names.add("Jan");
		names.add("Sara");
		names.add("Fred");
		names.add("Marc");
		
		System.out.println("Here's an ArrayList of Strings:\n");
		//iterating through list with standard for-loop
		for(int i = 0; i < names.size(); i++){
			String s = names.get(i);
			System.out.println("String at element " + i + " " + s);
		}
		System.out.println("\n");
		
		//iterating through list with for-each loop
		for(String s : names){
			System.out.println(s);
		}
		System.out.println("\n");
		
		System.out.println("Here's an ArrayList of Integers:\n");
		//ArrayList of Integers
		ArrayList<Integer> numbers = new ArrayList<Integer>();
		
		numbers.add(7);
		numbers.add(-24);
		numbers.add(1);
		numbers.add(1040);
		numbers.add(-2327);
		
		//iterating through list with standard for-loop
		for(int i = 0; i < numbers.size(); i++){
			Integer j = numbers.get(i);
			System.out.println(j);
		}
		System.out.println("\n");
		
		//iterating through list with for-each loop
		for(Integer j : numbers){
			System.out.println(j);
		}
		System.out.println("\n");
	}
}