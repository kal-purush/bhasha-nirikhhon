package com.chris.oscjp.chapter3;

public class Bertha {
	 static String s = "";
	 static void doStuff(Object o) { s += " 1 Object "; }
	 static void doStuff(Object... o) { s += " 2 Var-arg Object "; }
	 static void doStuff(Integer... i) { s += " 3 Var-arg Integer Wrapper "; }
	 static void doStuff(Long L) { s += " 4 Long Wrapper "; }
	 static void doStuff(Integer I) { s += " 5Integer Wrapper "; }
	 
	 public static void main(String[] args) {
	 int x = 4; 
	 Integer I = 8;
	 Boolean y = true; 
	 short[] sa= {1,2,3};
	 
	 System.out.println("Called with int, Boolean Wrapper ");
	 doStuff(x,y);
	 System.out.println(s);
	 System.out.println("Called with Boolean Wrapper ");
	 doStuff(y);
	 System.out.println(s);
	 System.out.println("Called with int ");
	 doStuff(x);
	 System.out.println(s);
	 System.out.println("Called with short[], short[] ");
	 doStuff(sa, sa);
	 System.out.println(s);
	 System.out.println("Called with Integer Wrapper ");
	 doStuff(I);
	 System.out.println(s);
	 }
 
}