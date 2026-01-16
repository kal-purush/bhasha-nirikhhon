package com.hibernate.test;

public class Test {
	private final int i;
	static int j=10;
	int k=0;
	Test(){
		System.out.println("from construcotr");
		System.out.println(k);
	}
	public static void getMessage(){
		System.out.println("From parent");
	}
	public void checkIn(Test t){
		System.out.println("Check in from Parent");
	}
	public void checkIn(Object t){
		System.out.println("Check in from Parent Object args");
	}
	
	{
		System.out.println("from initiliaze");
		i=0;
		System.out.println(j);
		System.out.println(k);
	}
	static {
		System.out.println("from static");
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
Test t= new Test();
	}

}
class Test1 extends Test{
	public static void getMessage(String message){
		System.out.println("From child");
	}
	public void checkIn(Test1 t){
		System.out.println("Check in from Parent");
	}
	public static void main(String[] args) {
		Test t= new Test1();
		t.checkIn(new Test());
		t.checkIn(null);
		t.checkIn(new Object());
		//t.getMessage("name");
	} 
	
	class Test2 extends Test1{
		
	}
}