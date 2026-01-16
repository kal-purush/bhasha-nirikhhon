package runner;

import cucumber.api.CucumberOptions;
import cucumber.api.testng.AbstractTestNGCucumberTests;
@CucumberOptions(features = {"src\\main\\resources\\Features\\cucum1.feature",
        "E:\\workspace\\Demo\\src\\main\\java\\runner\\steps\\TestSteps2.java"},
        glue = "TestSteps",monochrome = true)
public class TestRunner extends AbstractTestNGCucumberTests {
}