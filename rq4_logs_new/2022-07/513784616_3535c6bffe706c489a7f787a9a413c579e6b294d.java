package com.lti.demo;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.lti.entity.Calculator;

class BeforeAfterTests {

	int sum;
	Calculator c = new Calculator();
	
	@BeforeAll
	public static void testDbConn()
	{
		System.out.println("Db conn is up ");
	}
	
	@BeforeEach
	 public void testStartUp()
	 {
		 sum = 0;
		 System.out.println("sum = 0");
	 }
	
	

	@Test
	public void testAdd()
	{
		int r = c.add(100, 50);
		//Assertions.assertEquals(150,r);
		Assertions.assertEquals(150,r,"salemon bhoi");
	}
	@Test
	public void testAdd1()
	{
		int r = c.add(200, 50);
		//Assertions.assertEquals(150,r);
		Assertions.assertEquals(250,r,"salemon bhoi");
	}
	
	@AfterEach
	 public void testSHutDown()
	 {
		 sum = 0;
		 System.out.println("shutdown sum = 0");
	 }
	
	@AfterAll
	public static void testDbConnNull()
	{
		System.out.println("Db conn is null ");
	}
	
	}