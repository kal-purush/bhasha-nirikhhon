package Runner;

import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

@RunWith(Cucumber.class)
@CucumberOptions(
		       //features="C:\\Users\\z00467ka\\eclipse-workspace\\FreeCrmBDDFramework\\src\\main\\java\\Features",
		       //features="C:\\Users\\z00467ka\\eclipse-workspace\\FreeCrmBDDFramework\\src\\main\\java\\Features\\contacts.feature",
		       //features="C:\\Users\\z00467ka\\eclipse-workspace\\FreeCrmBDDFramework\\src\\main\\java\\Features\\deals.feature",
		       features="C:\\Users\\z00467ka\\eclipse-workspace\\FreeCrmBDDFramework\\src\\main\\java\\Features\\dealsmap.feature",
		       glue={"stepDefinations"},
		       plugin = {"pretty","html:test-output"},
		       //format={"pretty","html:test-output"}, deprecated not in use anymore
		       monochrome=true,
		       //monochrome=false,
		       strict=false,
		       dryRun=false
		       //dryRun=true;
		
		)

public class TestRunner 
{
     
}