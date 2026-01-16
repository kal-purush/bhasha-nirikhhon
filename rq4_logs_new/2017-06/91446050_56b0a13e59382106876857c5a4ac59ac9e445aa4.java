package com.liuqh.solrclient;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class TestTreeSet {
	public static void main(String[] args) {
		Set<A> set=new HashSet<A>();
		//Set<A> set=new TreeSet<A>();
		A a1= new A(1,2);
		A a2= new A(2,3);
		A a3= new A(1,0);
		A a4= new A(1,0);
		set.add(a1);
		set.add(a2);
		set.add(a3);
		set.add(a4);
		for(A temp:set){
			System.out.println(temp);
		}
		
	}
}
class A implements Comparable<A>{
	int a;
	int b;
	
	public A(int a, int b) {
		super();
		this.a = a;
		this.b = b;
	}



	@Override
	public String toString() {
		return "A [a=" + a + ", b=" + b + "]";
	}

	@Override
	public int compareTo(A o) {
		int a=this.a-o.a;
		/*if(a==0){
			a= this.b-o.b;							
		}*/
		return a;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + a;
		result = prime * result + b;
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		A other = (A) obj;
		if (a != other.a)
			return false;
		if (b != other.b)
			return false;
		return true;
	}
	
}