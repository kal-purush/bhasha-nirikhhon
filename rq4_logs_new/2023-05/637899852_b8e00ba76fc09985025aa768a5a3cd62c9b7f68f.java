package test.java;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import main.java.Factorer;

public class RefactoredTests {
	
	Factorer factorer;
	List<Integer> factors; 
	List<Integer> expectedResult;
	int numberToBeFactored;
	
	
	@Before
	public void setUp()  {
		factorer = new Factorer();
		factors = new ArrayList<Integer>(); 
		expectedResult = new ArrayList<Integer>();
		
	}
	
	@After
	public void teardown()  {
		factorer = null;
		factors = null;
		expectedResult = null;
	}
		

	@Test
	public void testConstruction() {
		
		assertNotNull(factorer);	
	}
	
	@Test
	public void testFactorsOf1() {
		
		numberToBeFactored = 1;
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf2() {

		numberToBeFactored = 2;
		expectedResult.add(2);
	
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf3() {
		
		numberToBeFactored = 3;
		expectedResult.add(3);
			
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf4() {

		numberToBeFactored = 4;
		expectedResult.add(2);
		expectedResult.add(2);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf5() {
		numberToBeFactored = 5;
		expectedResult.add(5);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf6() {
 
		numberToBeFactored = 6;
		expectedResult.add(2);
		expectedResult.add(3);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf7() {

		numberToBeFactored = 7;
		expectedResult.add(7);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOf8() {

		numberToBeFactored = 8;
		expectedResult.add(2);
		expectedResult.add(2);
		expectedResult.add(2);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}

	@Test
	public void testFactorsOf9() {

		numberToBeFactored = 9;
		expectedResult.add(3);
		expectedResult.add(3);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}
	
	@Test
	public void testFactorsOfBigNumber() {

		numberToBeFactored = 3*5*8*11*17*23*125;
		expectedResult.add(2);
		expectedResult.add(2);
		expectedResult.add(2);
		expectedResult.add(3);
		expectedResult.add(5);
		expectedResult.add(5);
		expectedResult.add(5);
		expectedResult.add(5);
		expectedResult.add(11);
		expectedResult.add(17);
		expectedResult.add(23);
		
		factors = factorer.factor(numberToBeFactored);
		
		Assert.assertEquals(expectedResult, factors);
	}

}