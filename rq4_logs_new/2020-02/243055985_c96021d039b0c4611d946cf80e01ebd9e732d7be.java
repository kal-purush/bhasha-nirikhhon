package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import pages.EmployeeDetailPage;
import pages.EmployeeTimesheetPage;
import utilities.Driver;
import utilities.Reusable;

public class Verify_empty_task_Internal_GAP_Analysis_Step_Defs {
    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();
    EmployeeTimesheetPage employeeTimesheetPage = new EmployeeTimesheetPage();

    @Given("User click on timesheet button")
    public void user_click_on_timesheet_button() {
       employeeDetailPage.timesheetButton.click();
    }

    @Given("User able to see timesheet page")
    public void user_able_to_see_timesheet_page() {
        Reusable.waitForVisibility(employeeTimesheetPage.createButton,10);
        String actualTitle = Driver.getDriver().getTitle();
        String expectedTitle = "Timesheets - Odoo";
        Assert.assertEquals("title doesn't match",expectedTitle,actualTitle);
    }

    @Given("User create a new timesheet")
    public void user_create_a_new_timesheet() {
        Reusable.waitForVisibility(employeeTimesheetPage.createButton,10);
        employeeTimesheetPage.createButton.click();
    }

    @Given("User add specific employee name \\(Antoine Langlais)")
    public void user_add_specific_employee_name_Antoine_Langlais() {
        employeeTimesheetPage.inputEmployeeName.sendKeys("Antoine Langlais");
    }

    @Given("User add description \\(test)")
    public void user_add_description_test() {
       employeeTimesheetPage.inputDescription.sendKeys("test");
    }

    @Given("User add name of project \\(Internal - GAP Analysis)")
    public void user_add_name_of_project_Internal_GAP_Analysis() {
        employeeTimesheetPage.inputProjectName.sendKeys("Internal - GAP Analysis");
    }

    @Then("verify the project has no task")
    public void verify_the_project_has_no_task() {
       String emptyTask = employeeTimesheetPage.inputTask.getText();
       Assert.assertTrue("it has value",emptyTask.isEmpty());
    }
}