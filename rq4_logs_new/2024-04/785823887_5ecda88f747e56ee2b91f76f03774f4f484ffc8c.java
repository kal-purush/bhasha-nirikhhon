package StepDefinitions;

import ApachePOI.resource.ExcelLogin_Mert;
import Pages.DialogContent_Mert;
import Pages.LeftNav_Mert;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

public class US_702_MertStepsApachePOI {
    DialogContent_Mert dcm = new DialogContent_Mert();
    LeftNav_Mert lfm = new LeftNav_Mert();

    @When("Click on the Sign In button")
    public void clickOnTheSignInButton() {
        lfm.myClick(lfm.SignInButton);
    }
    @Then("Finally, user must login to the account")
    public void finallyUserMustLoginToTheAccount() throws IOException {
        ExcelLogin_Mert.excelWord();
        dcm.myClick(lfm.LoginButton);
    }

    @Then("User should see successful login message")
    public void userShouldSeeSuccessfulLoginMessage() {
        lfm.verifyContainsText(dcm.confirmLogin, "Welcome");
    }

    @And("User should click on the Logout button")
    public void userShouldClickOnTheLogoutButton() {
        dcm.myClick(dcm.logoutClick);
        dcm.myClick(dcm.logoutButton);
        lfm.myClick(lfm.SignInButton);
    }

    @And("User should be able to see the Forgot my Password button")
    public void userShouldBeAbleToSeeTheForgotMyPasswordButton() {
        lfm.myClick(lfm.forgotMyPasswordButton);
    }
}