package com.XFleet.step_definitions;

import com.XFleet.pages.BasePage;
import com.XFleet.utilities.BrowserUtils;
import com.XFleet.utilities.Driver;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.By;

public class OroincDocStepDefs extends BasePage{

    @Given("the user click the question mark icon")
    public void the_user_click_the_question_mark_icon() {
        
        waitUntilLoaderScreenDisappear();
        Driver.getDriver().findElement(By.xpath("//a[@href='http://www.oroinc.com/doc']")).click();

    }

    @Then("the user access to the Oroinc Documentation page")
    public void the_user_access_to_the_oroinc_documentation_page() {

        for (String each : Driver.getDriver().getWindowHandles()) {
            Driver.getDriver().switchTo().window(each);
        }

        Assert.assertEquals(Driver.getDriver().getCurrentUrl(),"https://doc.oroinc.com/");

    }


}