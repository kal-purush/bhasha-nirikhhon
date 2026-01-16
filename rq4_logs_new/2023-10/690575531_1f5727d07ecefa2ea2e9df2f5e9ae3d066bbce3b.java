package logic.hooks;

import infra.WebDriverWrapper;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import io.qameta.allure.Allure;
import logic.api.RamiLeviApi;
import logic.context.TestContext;
import logic.pages.HomePage;
import utils.ReadFile;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import static infra.WebDriverWrapper.takeScreenshot;
import static logic.entites.enums.TestContextKey.*;
import static logic.entites.enums.Property.*;

public class MyHooks {
    private static TestContext testContext;
    private WebDriverWrapper driverWrapper;
    private String testUrl = "https://www.rami-levy.co.il/he";
    private String userEmail;
    private String userPassword;
    private String userName;
    private final String propertyFileName = "userData.properties";


    public MyHooks(TestContext testContext) {
        this.testContext = testContext;
    }

    @Before
    public void beforeScenario() {
        driverWrapper = new WebDriverWrapper();
        driverWrapper.getDriver().get(testUrl);
        // Maximize the browser window
        driverWrapper.getDriver().manage().window().maximize();

        // Read property file and get user data
        Properties prop = ReadFile.readPropertiesFile(propertyFileName);
        userEmail = prop.getProperty(PROPERTY_EMAIL.getKey());
        userPassword = prop.getProperty(PROPERTY_PASSWORD.getKey());
        userName = prop.getProperty(PROPERTY_NAME.getKey());

        testContext.put(KEY_DRIVER, driverWrapper.getDriver());
        testContext.put(KEY_EMAIL, userEmail);
        testContext.put(KEY_PASSWORD, userPassword);
        testContext.put(KEY_NAME, userName);
    }

    @After
    public void teardown(Scenario scenario) {
        if (scenario.isFailed()) {
            Allure.addAttachment("Failed Screenshot", new ByteArrayInputStream(takeScreenshot(scenario.getName(), testContext)));
        }
        if (testContext.get(ITEM_ID) != null)
            RamiLeviApi.deleteItemFromCart(testContext.get(ITEM_ID));
        testContext = null;
        // Close the WebDriver (close browser)
        driverWrapper.closeDriver();
    }
}