package step_definitions.briteERPEmployeeModule;


import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.Keys;
import pages.EmployeeTabPage;
import utilities.Reusable;

public class Add_employee {
    EmployeeTabPage employeeTabPage =new EmployeeTabPage();

    @Given("user click on search box")
    public void user_click_on_search_box() {
        employeeTabPage.search.click();

    }

    @Given("user click on favorite box")
    public void user_click_on_favorite_box() {
        employeeTabPage.favoriteBox.click();

    }

    @Given("user clicks add to dashboard")
    public void user_clicks_add_to_dashboard() {
        employeeTabPage.addDashboard.click();





    }

    @Then("user clicks on employee box")
    public void user_clicks_on_employee_box() {
        employeeTabPage.employeeBox.click();
        employeeTabPage.employeeBox.clear();
        Reusable.pause(2);
        String nameForEmployeeTab= "pumbik";
        employeeTabPage.employeeBox.sendKeys(nameForEmployeeTab+ Keys.ENTER);

    }

    @Then("user enter employee to dashboard")
    public void user_enter_employee_to_dashboard() {
        employeeTabPage.addEmployee.click();

    }

    @Then("user clicks to dashboard tab")
    public void user_clicks_to_dashboard_tab() {
        employeeTabPage.dashboardTab.click();


    }

    @Then("user should see name on employee on dashboard")
    public void user_should_see_name_on_employee_on_dashboard() {
        String nameForEmployeeTab= "pumbik";
        Assert.assertTrue(employeeTabPage.nameOfEmployeeModuleOnDashboard.getText().equals(nameForEmployeeTab));

    }


}