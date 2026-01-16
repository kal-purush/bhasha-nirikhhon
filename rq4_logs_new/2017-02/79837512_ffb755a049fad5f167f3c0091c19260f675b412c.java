package com.sj1.pr3.passgen;

import org.junit.Test;

public class PassGenTest {
	
	 /** 
	 * Test the Password generator by changing the variable values.
	 * 
	 * Error 1: If the minLength is larger than maxLength, there will be an error.
	 * Error 2: If the sum of desired chars are larger than the generated password length
	 * there will be another error.
	 * */
	
	@SuppressWarnings("static-access")
	@Test
	public void test() {
		int minLength = 5;
		int maxLength = 20;
		int nbrOfUPR = 3;
		int nbrOfLWR = 0;
		int nbrOfNBR = 3;
		int nbrOfSYM = 3;

		PassGen passgen = new PassGen();

		char[] test = passgen.genPwd(minLength, maxLength, nbrOfUPR, nbrOfLWR, nbrOfNBR, nbrOfSYM);
		System.out.println(test);
	}

}