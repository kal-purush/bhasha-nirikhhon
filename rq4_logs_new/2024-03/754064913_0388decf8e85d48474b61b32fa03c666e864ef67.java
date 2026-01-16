package com.methodoverriding;

public class Child extends Parent{
	
	public void show() {
		System.out.println("Child Show() is called");
	}
	
	//Static method
	public static void print() {
		System.out.println("Child static print() is called");
	}

}