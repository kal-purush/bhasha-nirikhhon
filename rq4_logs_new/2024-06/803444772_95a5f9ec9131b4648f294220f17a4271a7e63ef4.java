package StepDefinitions;

import Pages.DialogContent;
import Utilities.Tools;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.testng.Assert;

import javax.lang.model.element.Element;

public class _15_ProfileColorSteps {

    DialogContent dc=new DialogContent();
    @When("link to profile setting")
    public void linkToProfileSetting() {
        dc.myClick(dc.TopNavProfil);
        Tools.Wait(1);
        dc.myClick(dc.Settings);
        Tools.Wait(2);
    }

    @Then("must see theme menu")
    public void mustSeeThemeMenu() {
        Assert.assertTrue(dc.DefaultTheme.isDisplayed(),"Message can not be displayed");

    }

    @And("User should see multiple theme options")
    public void userShouldSeeMultipleThemeOptions() {
        dc.myClick(dc.DefaultTheme);
        Assert.assertTrue(dc.DefaultTheme2.isDisplayed(),"Message can not be displayed");
        Assert.assertTrue(dc.PurpleTheme.isDisplayed(),"Message can not be displayed");
        Assert.assertTrue(dc.DarkPurpleTheme.isDisplayed(),"Message can not be displayed");
        Assert.assertTrue(dc.IndigoTheme.isDisplayed(),"Message can not be displayed");

    }

    @And("User should be able to set option first as default")
    public void userShouldBeAbleToSetOptionFirstAsDefault() {
        dc.myClick(dc.PurpleTheme);
        dc.myClick(dc.SaveButton2);

    }

   @Then("User clicks Save button and sees Success message")
   public void userClicksSaveButtonAndSeesSuccessMessage() {
       dc.wait.until(ExpectedConditions.visibilityOf(dc.SuccessMsg2));
       Assert.assertTrue(dc.SuccessMsg2.isDisplayed(),"Message can not be displayed");
   }
}