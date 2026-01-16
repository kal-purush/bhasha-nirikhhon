package no.hvl.dat108;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import no.hvl.dat108.Validator;


public class ValidatorTest {

	private String redirect1;
	private String redirect2;
	private String htmlTag1;
	private String htmlTag2;
	private String htmlTag3;
	private String htmlTag4;
	private String whitespace1;
	private String whitespace2;
	private String whitespace3;
	
	@Before
	public void prepare() {
		redirect1 = "You need to log in.";
		redirect2 = "Invalid password.";
		htmlTag1 = "<h1>here</h1>";
		htmlTag2 = "here";
		htmlTag3 = "<script>et angrep</script>";
		htmlTag4 = "";
		whitespace1 = " ";
		whitespace2 = "a b";
		whitespace3 = " ab";
	}
	
	@After
	public void tearDown() {
		redirect1 = null;
		redirect2 = null;
		htmlTag1 = null;
		htmlTag2 = null;
		htmlTag3 = null;
		htmlTag4 = null;
		whitespace1 = null;
		whitespace2 = null;
		whitespace3 = null;
	}
	
	@Test
	public void getErrorMessageTest() {
		
		String a = Validator.getErrorMessage(true,false);
		String b = Validator.getErrorMessage(false, true);
			
		assertTrue(a.matches(redirect1));
		assertTrue(b.matches(redirect2));
		
	}
	
	@Test
	public void cleanInputTest() {
		String a = Validator.cleanInput(htmlTag1);
		String b = Validator.cleanInput(htmlTag3);
		System.out.println(b);
		assertTrue(a.equals(htmlTag2));
		assertTrue(b.equals(htmlTag4));
	}
	
	@Test
	public void isInvalidTest() {
		boolean a = Validator.isInvalid(whitespace1);
		boolean b = Validator.isInvalid(whitespace2);
		boolean c = Validator.isInvalid(whitespace3);
		
		assertTrue(a);
		assertFalse(b);
		assertFalse(c);
	}
	
	
}