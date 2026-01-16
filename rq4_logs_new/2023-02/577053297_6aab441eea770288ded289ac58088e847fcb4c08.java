package arcadia.stepdefinations;

import arcadia.context.TestContext;
import arcadia.pages.ComponentDB.HouseKeeping.cleanComponentDB;
import arcadia.utils.ConversionUtil;
import io.cucumber.java.en.Then;

public class HouseKeepingStepDefinitions {
    private final TestContext context;
    ConversionUtil conversionUtil  = new ConversionUtil();
    public HouseKeepingStepDefinitions(TestContext context) {
        this.context = context;
    }

    @Then("House keeping invoked for {string}")
    public void house_keeping_invoked_for(String component) throws InterruptedException {
        switch(component.toLowerCase()) {
            case "componentdb":
                new cleanComponentDB(context.driver).initiateComponentDBHouseKeeping();
                break;
        }
    }
}