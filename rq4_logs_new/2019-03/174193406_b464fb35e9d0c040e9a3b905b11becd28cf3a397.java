package runner;

//import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.testng.AbstractTestNGCucumberTests;

//@RunWith(Cucumber.class)
@CucumberOptions(
				features ="feature",
				glue = "stepDefiniton")
				 //plugin = {"cucumber-html-report"})
public class TestRunner extends AbstractTestNGCucumberTests {

}