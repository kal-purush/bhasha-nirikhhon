package com.hudl.Runners;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "pretty",
                "json:target/jsonReport/cucumber.json",
                "de.monochromata.cucumber.report.PrettyReports:target/cucumber"},
//        plugin = {"json:target/cucumber.json",
//                "html:target/default-html-reports",
//                "rerun:target/rerun.txt"},
        features = "src/test/resources/features",
        glue = "com/hudl/stepDefinitions",
        dryRun = false,
        tags = "@login "
)



public class CukesRunner {
}