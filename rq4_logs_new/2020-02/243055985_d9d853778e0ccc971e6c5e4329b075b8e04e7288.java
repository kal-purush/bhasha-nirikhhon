package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Then;
import org.junit.Assert;
import pages.EmployeeDetailPage;
import pages.EmployeeTabPage;

public class Ability_to_see_OC {

    EmployeeTabPage employeeTabPage =new EmployeeTabPage();
    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();

    @Then("User should be able to see Organization Chart")
    public void user_should_be_able_to_see_Organization_Chart() {
        Assert.assertTrue("Organization Chart is NOT displayed",employeeDetailPage.organizationChart.isDisplayed());
    }
}