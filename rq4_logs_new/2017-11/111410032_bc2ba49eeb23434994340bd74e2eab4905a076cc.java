package junit_demo;

import static org.junit.Assert.*;

import org.junit.Test;

public class SampleTest2 {
	
	@Test
	public void substraction(){
	
	int a=10;
	int b=5;
	assertTrue(a - b==5);
	}
	
	@Test
	public void multiplication(){
		
		int a=10;
		int b=5;
		assertTrue(a * b==50);
		}
	@Test
	public void division(){
		
		int a=10;
		int b=5;
		assertTrue(a / b==5);
		}	
}