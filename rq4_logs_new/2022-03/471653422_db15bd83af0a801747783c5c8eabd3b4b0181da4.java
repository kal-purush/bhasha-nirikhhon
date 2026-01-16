package com.sgtesting.inheritance;

class A
{
	void printA()
	{
		System.out.println("Class A");
	}
}
class B extends A
{
	void printB()
	{
		System.out.println("Class B");
	}
}
class C extends A
{
	void printC()
	{
		System.out.println("Class C");
	}
}
public class Hierarchicalinheritance{
public static void main(String[] arsgs) {
	B objb=new B();
	objb.printA();
	objb.printB();
	
	C objc=new C();
	objc.printA();
	objc.printC(); 
}
}