package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Then;
import org.junit.Assert;
import pages.EmployeeDetailPage;
import pages.EmployeeTabPage;

public class Verify_details_displayed_in_EDP {
    EmployeeTabPage employeeTabPage = new EmployeeTabPage();
    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();

    // Verification of details to be displayed
    @Then("User should see all details of EDP")
    public void user_should_see_all_details_of_EDP() {
        Assert.assertTrue(employeeDetailPage.nameOfEmployeeInHeader.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workInformation_tab.isDisplayed());
        Assert.assertTrue(employeeDetailPage.contactInformation.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workAddress.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workLocation.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workEmail.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workMobile.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workPhone.isDisplayed());
        Assert.assertTrue(employeeDetailPage.position.isDisplayed());
        Assert.assertTrue(employeeDetailPage.department.isDisplayed());
        Assert.assertTrue(employeeDetailPage.jobPosition.isDisplayed());
        Assert.assertTrue(employeeDetailPage.manager.isDisplayed());
        Assert.assertTrue(employeeDetailPage.coach.isDisplayed());
        Assert.assertTrue(employeeDetailPage.workingHours.isDisplayed());
    }

}