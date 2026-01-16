package com.XFleet.step_definitions;

import com.XFleet.pages.BasePage;
import com.XFleet.pages.LoginPage;
import com.XFleet.pages.VehicleModelPage;
import com.XFleet.utilities.ConfigurationReader;
import com.XFleet.utilities.Driver;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.ArrayList;
import java.util.List;


public class VehicleModelPage_StepDefinitions extends BasePage {

    LoginPage login = new LoginPage();
    VehicleModelPage vehicleModelPage = new VehicleModelPage();

    @Given("The user is on log in page")
    public void the_user_is_on_log_in_page(){

    }

    @When("user logs in as store manager")
    public void userLogsInAsStoreManager() {

        login.login(ConfigurationReader.getProperty("store_manager_username"),ConfigurationReader.getProperty("store_manager_password"));

        waitUntilLoaderScreenDisappear();
    }

    @And("user clicks on Fleet module")
    public void userClicksOnFleetModule() {

        vehicleModelPage.fleet.click();

    }

    @Then("user clicks on Vehicle Model option")
    public void userClicksOnVehicleModelOption() {

        vehicleModelPage.vehiclesModel.click();
        waitUntilLoaderScreenDisappear();

    }


    @Then("the user sees {int} columns -MODEL NAME, MAKE, CAN BE REQUESTED, CVVI, CO{int} FEE \\(MONTH), COST \\(DEPRECIATED), TOTAL COST \\(DEPRECIATED), CO{int} EMISSIONS, FUEL TYPE, VENDORS")
    public void theUserSeesColumnsMODELNAMEMAKECANBEREQUESTEDCVVICOFEEMONTHCOSTDEPRECIATEDTOTALCOSTDEPRECIATEDCOEMISSIONSFUELTYPEVENDORS(int arg0, int arg1, int arg2) {

        waitUntilLoaderScreenDisappear();
        String expectedTitle= "MODEL NAME, MAKE, CAN BE REQUESTED, CVVI, CO2 FEE (MONTH), COST (DEPRECIATED), TOTAL COST (DEPRECIATED), CO2 EMISSIONS, FUEL TYPE, VENDORS";
        List<WebElement> webElementList = Driver.getDriver().findElements(By.xpath("//th[contains(@class,'sortable')]"));

        List<String> strings = new ArrayList<String>();
        for(WebElement e : webElementList){
            strings.add(e.getText());
        }
        String listString = String.join(", ", strings);

        Assert.assertFalse(listString.contains(expectedTitle));
    }

    @When("user logs in as sales manager")
    public void userLogsInAsSalesManager() {

        login.login(ConfigurationReader.getProperty("sales_manager_username"),ConfigurationReader.getProperty("store_manager_password"));

    }



    @When("user logs in as driver")
    public void userLogsInAsDriver() {
        login.login(ConfigurationReader.getProperty("driver_username"),ConfigurationReader.getProperty("store_manager_password"));


    }



    @Then("user sees You do not have permission to perform this action. message screen")
    public void userSeesYouDoNotHavePermissionToPerformThisActionMessageScreen() {

        String expectedMessage = "You do not have permission to perform this action.";
        String actualMessage = vehicleModelPage.warningMessage.getText();

        Assert.assertEquals(actualMessage,expectedMessage);
    }



}