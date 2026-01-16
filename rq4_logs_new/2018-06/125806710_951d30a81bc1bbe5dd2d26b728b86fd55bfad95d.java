package com.arrayClass;

public class Demo {
	public static void swap(Integer i, Integer j) {
		Integer temp=new Integer(i);
		i=j;
		j=temp;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Integer i=10;
		Integer j=20;
		swap(i, j);
		System.out.println(i +" "+ j);

	}

}