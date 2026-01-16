package com.qa.rest.tests;

import org.testng.annotations.Test;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;          //These two imports you have to write manually for hamcrest core and hamcrest library 

import java.util.ArrayList;

public class GetCallBDD {
	
	@Test
	public void test_numberOfCircuitsFor2017_Season() {
		
//		ArrayList<Argument> list = new ArrayList<>();
//		list.add(Argument.arg("MRData.CircuitTable.Circuits.circuitId"));
//		list.add(Argument.arg(hasSize(20)));
		
//		given().
//		when().
//		then().
//		assert().      //This is the sequence we have to write for Rest Assured BDD framework 
		
		
		given().
		when().
			get("http://ergast.com/api/f1/2017/circuits.json\n").    //this GET method will call this particular api
		then().
			assertThat().
			statusCode(200).
			and().             //We can assert number things with AND operator
			body("MRData.CircuitTable.Circuits.circuitId", hasSize(20)).  //Coming from hamcrest
			and(). //AND operator to assert few things
			header("Content-Length", equalTo("4551"));
			
		
		
		
		
		
		
		
		
		
	}
		

}