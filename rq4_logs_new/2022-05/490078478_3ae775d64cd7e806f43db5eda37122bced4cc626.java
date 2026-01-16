package Runner;

import org.junit.runner.RunWith;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

@RunWith(Cucumber.class)
@CucumberOptions(
        features = "src/test/java/Features"
        ,glue= {"Steps"},
        plugin={"html:target/cucumber-html-report", "json:target/cucumber.json",
                "pretty:target/cucumber-pretty.txt","usage:target/cucumber-usage.json",
                "junit:target/cucumber- results.xml"},
        tags={"@Regression"})
public class TestRunner {
}


