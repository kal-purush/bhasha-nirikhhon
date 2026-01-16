package com.qa.main.entities;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

@SpringBootTest
public class CustomerTest {
	
	@Test
	public void equalsNullTest() {
		Customer customer = new Customer("Anoush", "Lowton", 29);
		
		assertFalse(customer.equals(null));
	}
	
	@Test
	public void differentClassTest() {
		Customer customer = new Customer("Anoush", "Lowton", 29);
		String testString = "Hi";
		
		assertFalse(customer.equals(testString));
	}
	
	@Test
	public void checkHashTest() {
		Customer one = new Customer("Anoush", "Lowton", 29);
		Customer two = new Customer("Anoush", "Lowton", 29);
		
		assertTrue(one.hashCode() == two.hashCode());
	}
}