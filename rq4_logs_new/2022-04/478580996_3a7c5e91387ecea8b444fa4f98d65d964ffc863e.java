package runners;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;

@CucumberOptions(
       features = {"src/test/resources/features"},
               glue = {"steps"},
               plugin = {"pretty",
               "html:target/cucumber.html",
               "json:target/cucumber.json"}
               )
public class BookingTest extends AbstractTestNGCucumberTests {
}