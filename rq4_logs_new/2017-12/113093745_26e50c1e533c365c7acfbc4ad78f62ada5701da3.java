package com.transavia.bdd.runner;


import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        format = { "pretty", "html:target/cucumber" },
        glue = "com.transavia.bdd.steps",
        features = "classpath:cucumber/transavia.feature"
)
public class TransaviaTest {
}