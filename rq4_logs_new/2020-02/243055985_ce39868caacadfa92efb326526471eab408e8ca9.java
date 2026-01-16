package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.Keys;
import pages.EmployeeDetailPage;
import pages.EmployeeTabPage;
import utilities.Reusable;

public class Log_note_message_step_Defs {

    EmployeeDetailPage employeeDetailPage = new EmployeeDetailPage();

    @When("User clicks on log note button")
    public void user_clicks_on_log_note_button() {
        Reusable.waitForVisibility(employeeDetailPage.logNoteButton,10);
        employeeDetailPage.logNoteButton.click();

    }

    @When("User enters a log note text")
    public void user_enters_a_log_note_text() {
        Reusable.waitForVisibility(employeeDetailPage.logNoteMessageBox,10);
        employeeDetailPage.logNoteMessageBox.sendKeys("Something texted" + Keys.ENTER);
        employeeDetailPage.finalLogButton.click();
    }

    @Then("User should be able to see sent log note as most recent note")
    public void user_should_be_able_to_see_sent_log_note_as_most_recent_note() {
        Reusable.waitForVisibility(employeeDetailPage.mostRecentLogNote,10);
    String recentLognote = employeeDetailPage.mostRecentLogNote.getText();
    String expected = "Something texted";
        Assert.assertEquals(expected,recentLognote);


    }






}

