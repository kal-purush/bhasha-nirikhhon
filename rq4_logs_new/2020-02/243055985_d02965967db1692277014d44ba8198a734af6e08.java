package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.interactions.Actions;
import pages.EmployeeDetailPage;
import utilities.Driver;

public class MarkMassageAsTodoStepDefs {

    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();
    Actions action = new Actions(Driver.getDriver());

    @Given("user locate and hover message that he wants to Mark as TODO")
    public void user_locate_and_hover_message_that_he_wants_to_Mark_as_TODO() {
        action.moveToElement(employeeDetailPage.lastMessage).build().perform();
    }

    @Given("user clicks on STAR icon")
    public void user_clicks_on_STAR_icon() {
        employeeDetailPage.starForLastMessage.click();
        action.moveToElement(employeeDetailPage.employeeNameInHeader).build().perform();
    }

    @Then("that message should be marked with star as TODO")
    public void that_message_should_be_marked_with_star_as_TODO() {
        Assert.assertTrue(employeeDetailPage.starForLastMessage.isDisplayed());
        // unclicking star
        employeeDetailPage.starForLastMessage.click();
    }


}