package com.xworkz.starter;

import com.xworkz.starter.event.Emailoperator;
import com.xworkz.starter.exception.EmailValidationException;

public class EmailScammer {

	public static void main(String[] args) throws EmailValidationException {
Emailoperator emailoperator=new Emailoperator();
		
		emailoperator.save("shivsai@gmail.com");
		emailoperator.save("shiv@gmail.com");
		emailoperator.save("sai@gmail.com");
		emailoperator.save("kiran@gmail.com");
		emailoperator.save("saikiran@gmail.com");

	}

}