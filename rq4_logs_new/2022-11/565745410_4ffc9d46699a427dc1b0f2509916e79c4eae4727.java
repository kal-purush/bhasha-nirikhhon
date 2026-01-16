package com.lti.demo;

public class Calculator {
	public String sayHello() {
		return "hello";
	}
	public int add(int x, int y) {
		int result=x+y;
		return result;
	}
	
	public void searchEmpId(int empid)
    {
        if(empid==0)
        {
            throw new ArithmeticException("You enetered zero");
        }
        else
        {
            System.out.println(" do something");
        }
        
    }

}