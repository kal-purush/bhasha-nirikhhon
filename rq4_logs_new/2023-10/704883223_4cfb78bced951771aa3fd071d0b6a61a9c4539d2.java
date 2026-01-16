package Runners;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;
@CucumberOptions(
        features = {"src/test/java/FeatureFiles/_01_PositionCategorieManagement.feature"},
        glue = {"StepDefinitions"}
)
public class _01_Runner extends AbstractTestNGCucumberTests {

}