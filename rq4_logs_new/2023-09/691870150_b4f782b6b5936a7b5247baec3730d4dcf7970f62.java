package com.hex.smart.api.basetest;

import org.junit.BeforeClass;

import io.restassured.RestAssured;

public class BaseClassTest {

	@BeforeClass
	public static void init() {
		
		RestAssured.baseURI="http://localhost:8080";
		RestAssured.basePath="/api/users/";
		
	}
}