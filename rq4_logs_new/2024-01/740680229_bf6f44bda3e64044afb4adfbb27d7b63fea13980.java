package com.XFleet.step_definitions;

import com.XFleet.utilities.BrowserUtils;
import com.XFleet.utilities.Driver;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.ArrayList;
import java.util.List;

public class mainModuleAccessVerif {
    @Then("On the Main Page user can access all the main modules of the app as a {string}")
    public void on_the_main_page_user_can_access_all_the_main_modules_of_the_app_as_a(String userType, List<String> expectedListOfModuleNames) {
        BrowserUtils.waitFor(5);
        List<WebElement> webElementList = Driver.getDriver().findElements(By.xpath("//li[contains(@class,'dropdown dropdown-level-1')]"));
        List<String> actualListOfModuleNames = new ArrayList<>();
        for (WebElement each : webElementList) {
            actualListOfModuleNames.add(each.getText());
        }
        System.out.println("userType = " + userType);
        System.out.println("actualMonth_as_String = " + actualListOfModuleNames);

        Assert.assertEquals(actualListOfModuleNames,expectedListOfModuleNames);
    }
}
