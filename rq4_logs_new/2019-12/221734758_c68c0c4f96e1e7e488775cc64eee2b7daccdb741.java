package com.test.java8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class ComparatorUsingLambda {
	int id;
	String name;
	int age;
	
	public ComparatorUsingLambda(int id, String name, int age) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}
	
	public static void main(String[] args) {
		ArrayList<ComparatorUsingLambda> list1 = new ArrayList<ComparatorUsingLambda>();
		list1.add(new ComparatorUsingLambda(1, "Vishnu", 31));
		list1.add(new ComparatorUsingLambda(2, "Tamil", 23));
		list1.add(new ComparatorUsingLambda(3, "Kannan", 33));
		list1.add(new ComparatorUsingLambda(4, "Ravi", 13));
		list1.add(new ComparatorUsingLambda(5, "Vimal", 43));
		
		Collections.sort(list1, (id1, id2) -> id1.name.compareTo(id2.name));
		list1.forEach(value -> System.out.println(value.id+" "+value.age+" "+value.name));
		
		System.out.println(" Just a break...");
		list1.stream().filter(p -> p.age > 30).forEach(p -> System.out.println(p.id + " "+p.name + " "+p.age));
		
		System.out.println("Yet another processing");
		Stream<ComparatorUsingLambda> list2 = list1.stream().filter(p -> p.name.startsWith("V"));
		list2.forEach(value -> System.out.println(value.id+" "+value.age+" "+value.name));
		
	}

}