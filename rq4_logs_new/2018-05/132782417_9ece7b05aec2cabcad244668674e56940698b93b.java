package testcucumber;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.WebDriverWait;
import utils.Driver;
import utils.PropertyReader;
import utils.helpers.WaitingsHelpers;

import java.util.concurrent.TimeUnit;

public final class BaseSteps {

    private int baseTimeout = Integer.parseInt(PropertyReader.
            getPropertyFromFile("properties/settings.properties", "timeout"));

    static WebDriver driver;
    static WebDriverWait webDriverWait;

    public static WebDriver getDriver(){
        return driver;
    }

    public static WebDriverWait getWebDriverWait(){
        return webDriverWait;
    }

    @Before
    public void before(){
        driver = Driver.getDriver();
        driver.manage().timeouts().implicitlyWait(baseTimeout, TimeUnit.SECONDS);
        webDriverWait = WaitingsHelpers.getInstanceWebDriverWait(driver);
    }

    @After
    public void after(){
        driver.quit();
    }
}