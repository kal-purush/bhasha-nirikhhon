package helper;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import models.BaseModel;
import services.BaseService;
import services.helper.ReflectionHelper;

public class ReflectionHelperTest {

	private ReflectionHelper<TestModel> reflectionHelper;
	
	@Before
	public void setUp() throws Exception {
		reflectionHelper = new ReflectionHelper<>();
	}


	@Test
	public void testGetClass_withCorrectService_shouldReturnCorrect() {
		TestService testService = new TestService();
		
		assertEquals(reflectionHelper.getClass(testService),TestModel.class);
	}
	
	@Test
	public void testGetClass_withWrongObjectWithoutGeneralization_shouldReturnNull() {		
		assertNull(reflectionHelper.getClass(new Object()));
	}

	@Test
	public void testGetClass_withNull_shouldReturnNull() {
		assertNull(reflectionHelper.getClass(null));
	}
	
	private static class TestService extends BaseService<TestModel>{
		@Override
		protected String orderedBy() {
			return "test";
		}
	}
	
	private static class TestModel extends BaseModel{
	}
}