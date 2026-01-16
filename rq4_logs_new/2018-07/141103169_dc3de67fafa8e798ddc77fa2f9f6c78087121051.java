package stepdefinitions;

import cucumber.api.PendingException;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

public class LoginSteps {

    @Given("^I have opened homepage$")
    public void iHaveOpenedHomepage() throws Throwable {
        System.out.println("OPEN HOMEPAGE");
    }

    @When("^I select My account menu$")
    public void iSelectMyAccountMenu() throws Throwable {
        selectMyAccountMenu();
    }

    @Then("^I select Login button from login form$")
    public void iSelectLoginFromMyAccountMenu() throws Throwable {
        selectLogInButtonAccountMenu();
    }

    @And("^I enter Email address in login form$")
    public void iEnterEmailAddressInLogin() throws Throwable {
        enterEmailAddressLogin();
    }

    @And("^I enter Password in login form$")
    public void iEnterPasswordLogin() throws Throwable {
        enterPasswordLogin();
    }

    @Then("^I select Login button from login form$")
    public void iSelectLoginbutton() throws Throwable {
        selectLogInButtonLoginForm();
    }

    @And("^user account page is opened$")
    public void userAccountPageIsOpened() throws Throwable {
        System.out.println("SUCCESSFULLY LOGGED IN!");
    }
}