package com.sgtesting.actitime.driverscript;


import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import cucumber.api.CucumberOptions;
import cucumber.api.testng.CucumberFeatureWrapper;
import cucumber.api.testng.TestNGCucumberRunner;
@CucumberOptions(
		features="src/test/resources/featuresfile",
		glue="com.sgtseting.actitime.stepdefination"
)
public class DriverScript {
	
	private TestNGCucumberRunner testCucumberRunner=null;
	
	@BeforeClass(alwaysRun = true)
	public void setUpClass() throws Exception
	{
		testCucumberRunner=new TestNGCucumberRunner(this.getClass());
	}
	
	@Test(dataProvider="features")
	public void feature(CucumberFeatureWrapper cucumberFeature)
	{
		testCucumberRunner.runCucumber(cucumberFeature.getCucumberFeature());
	}
	
	@DataProvider
	public Object[][] features()
	{
		if(testCucumberRunner==null)
		{
			testCucumberRunner=new TestNGCucumberRunner(this.getClass());
		}
		return testCucumberRunner.provideFeatures();
	}
	@AfterClass(alwaysRun = true)
	public void tearDownClass() throws Exception
	{
		testCucumberRunner.finish();
	}

}