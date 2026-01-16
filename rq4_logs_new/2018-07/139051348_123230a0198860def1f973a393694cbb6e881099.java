package com.list;

import java.util.ArrayList;

public class ArrayListt {

	public static void main(String[] args) {
		
		ListEmployee li = new ListEmployee(12, "Vikas");
		ListEmployee l2 = new ListEmployee(13, "KKKKK");
		ArrayList<ListEmployee> list = new ArrayList<>();
		
		list.add(li);
		list.add(l2);
		System.out.println(list);

	}

}