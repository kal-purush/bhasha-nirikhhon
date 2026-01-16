package StepDefinitions;

import Pages.DialogContent;
import Pages.LeftNav;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.support.ui.Select;

public class AddingFields {
    DialogContent dc=new DialogContent();
    LeftNav ln=new LeftNav();

    @Given("Navigate to fields")
    public void navigateToFields() {
        ln.myClick(ln.setup);
        ln.myClick(ln.parameters);
        ln.myClick(ln.fields);
    }

    @When("Add a new field")
    public void addANewField() {
        dc.myClick(dc.addButton);
        dc.mySendKeys(dc.nameInput,"mmyy");
        dc.mySendKeys(dc.codeInput,"0202");
        dc.myClick(dc.fieldType);
        Select menu = new Select(dc.fieldTypes);
        menu.selectByIndex(4);
    }

    @Then("Edit the field")
    public void editTheField() {
    }

    @And("Delete the field")
    public void deleteTheField() {
    }

    @And("The field should be deleted successfully")
    public void theFieldShouldBeDeletedSuccessfully() {
    }
}