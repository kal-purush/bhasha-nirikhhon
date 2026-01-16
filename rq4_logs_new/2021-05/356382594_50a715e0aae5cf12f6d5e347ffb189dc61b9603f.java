package cucumber.stepdefs;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class EditProfileStepDefs {

    @Given("I am registered user")
    public void i_am_registered_user() {
        System.out.println(" I am registered.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I am logged in")
    public void i_am_logged_in() {
        System.out.println("I am logged in.");
//        throw new io.cucumber.java.PendingException();

    }

    @When("I navigate to {string}")
    public void iNavigateToProfile() {
        System.out.println("I get on the page Profile.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I select {string}")
    public void iSelectEdit() {
        System.out.println("I can edit the profile.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I enter {string} in field with id {string}")
    public void iEnterTestUsernameInFieldWithIdUsername() {
        System.out.println("I can change my Username.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I click button with id {string}")
    public void iClickButtonWithIdSave() {
        System.out.println("I save my changes.");
//        throw new io.cucumber.java.PendingException();

    }

    @Then("I get a success message {string} displayed")
    public void iGetASuccessMessageProfileSuccessfullyUpdatedDisplayed() {
        System.out.println("My changes are accepted.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I get redirected to {string}")
    public void iGetRedirectedToProfile() {
        System.out.println("I get to the page profile again.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I enter {string} in field with id {string}")
    public void iEnterExistingUsernameInFieldWithIdUsername() {
        System.out.println("I change my Username.");
//        throw new io.cucumber.java.PendingException();

    }

    @Then("I get an error message {string} displayed")
    public void iGetAnErrorMessageThisUsernameAlreadyExistsPleaseEnterDifferentUsernameDisplayed() {
        System.out.println("My changes are declined.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I get back to {string} page")
    public void iGetBackToEditProfilePage() {
        System.out.println("Get back to profile page.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I enter {string} in field with id {string}")
    public void iEnterTestDomainTldInFieldWithIdEmail() {
        System.out.println("I change my Email adress.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I enter {string} in field with id {string}")
    public void iEnterTestTestInFieldWithIdEmail() {
        System.out.println("I change my email adress.");
//        throw new io.cucumber.java.PendingException();

    }

    @Then("I get an error message {string} displayed")
    public void iGetAnErrorMessagePleaseEnterACorrectEmailAddressDisplayed() {
        System.out.println("Changed email adress is declined.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I enter {string} in field with id {string}")
    public void iEnterTestPasswordInFieldWithIdPassword() {
        System.out.println("Enter password.");
//        throw new io.cucumber.java.PendingException();
    }

    @And("I click {string}")
    public void iClickAddProfilePicture() {
        System.out.println("Add profile picture.");
//        throw new io.cucumber.java.PendingException();

    }

    @And("I choose a picture to upload")
    public void iChooseAPictureToUpload() {
        System.out.println("Choosed picture to upload.");
//        throw new io.cucumber.java.PendingException();

    }
}
