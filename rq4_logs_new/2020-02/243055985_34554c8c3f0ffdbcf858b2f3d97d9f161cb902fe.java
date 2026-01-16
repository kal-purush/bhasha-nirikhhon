package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import pages.EmployeeDetailPage;
import pages.EmployeeTabPage;
import utilities.Reusable;

public class Send_message_in_EDP {
    EmployeeTabPage employeeTabPage = new EmployeeTabPage();
    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();

    @When("User clicks on Send message")
    public void user_clicks_on_Send_message() {
        employeeDetailPage.sendMessageButton.click();
    }

    @When("User enters any text")
    public void user_enters_any_text() {
        String expectedText = "This is Cucumber-JUnit message";
        employeeDetailPage.messageBox.sendKeys(expectedText);
    }

    @When("User clicks on Send button")
    public void user_clicks_on_Send_button() {
       employeeDetailPage.sendButtonFinal.click();
        Reusable.pause(3);
    }

    @Then("User should see entered text as the most recent message")
    public void user_should_see_entered_text_as_the_most_recent_message() {
        String expectedText = "This is Cucumber-JUnit message";
        Assert.assertTrue("The text you've just entered is WRONG!",employeeDetailPage.mostRecentMessage.getText().equals(expectedText));
    }

}