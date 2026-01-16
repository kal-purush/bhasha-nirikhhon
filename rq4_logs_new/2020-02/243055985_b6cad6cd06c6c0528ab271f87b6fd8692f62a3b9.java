package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import pages.EmployeeTabPage;
import pages.EmployeeTimesheetPage;
import utilities.Reusable;

public class Create_timesheet_Step_Defs {
    EmployeeTimesheetPage employeeTimesheetPage = new EmployeeTimesheetPage();

    @And("User clicks on the Timesheets module in the right top corner of the Employee profile")
    public void user_clicks_on_the_Timesheets_module_in_the_right_top_corner_of_the_Employee_profile() {
        employeeTimesheetPage.btnTimesheets.click();
    }
    @And("User clicks on the Create button in the left top corner of the Timesheets page")
    public void user_clicks_on_the_Create_button_in_the_left_top_corner_of_the_Timesheets_page() {
        employeeTimesheetPage.btnCreate.click();
    }
    @And("User clicks on the Employee field to choose Employee name in the dropdown menu")
    public void user_clicks_on_the_Employee_field_to_choose_Employee_name_in_the_dropdown_menu() {
        employeeTimesheetPage.btnEmployee.click();
    }
    @And("User clicks on the Employee's name to select")
    public void user_clicks_on_the_Employee_s_name_to_select() {
        employeeTimesheetPage.selectEmployeeName.click();
    }
    @And("User enters text in the Description field")
    public void user_enters_text_in_the_Description_field() {
        employeeTimesheetPage.descriptionField.sendKeys("TPS reports");
    }
    @And("User clicks on the Project field to choose a project in the dropdown menu")
    public void user_clicks_on_the_Project_field_to_choose_a_project_in_the_dropdown_menu() {
        employeeTimesheetPage.projectField.click();
    }
    @And("User clicks on the Project to select")
    public void user_clicks_on_the_Project_to_select() {
        employeeTimesheetPage.projectTab.click();
    }
    @And("User clicks on the Task field")
    public void user_clicks_on_the_Task_field_to_choose_a_task() {
        employeeTimesheetPage.taskField.click();
    }
    @And("User chooses a Task")
    public void user_chooses_a_task() {
        employeeTimesheetPage.taskTab.click();
    }
    @And("User clicks on the Time Hours field")
    public void user_clicks_on_the_Time_Hours_field() {
        employeeTimesheetPage.timeField.clear();
    }
    @And("User enters amount of hours")
    public void user_enters_amount_of_hours() {
        employeeTimesheetPage.timeField.sendKeys("03:00");
    }
    @Then("User clicks on the Save button in the left top corner of the Timesheets page")
    public void user_clicks_on_the_Save_button_in_the_left_top_corner_of_the_Timesheets_page() {
        employeeTimesheetPage.saveButton.click();
    }
}