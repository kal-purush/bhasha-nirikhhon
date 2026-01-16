package general.commonAPI;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by shamimchy on 9/18/16.
 */
public class Base {

    public WebDriver webDriver = null;

    @Parameters({"url"})
    @BeforeMethod
    public void setUp(String url){
        System.setProperty("webdriver.gecko.driver", "/Users/shamimchy/Documents/geckodriver");
        DesiredCapabilities capabilities = DesiredCapabilities.firefox();
        capabilities.setCapability("marionette", true);

        webDriver = new FirefoxDriver( );
//        System.setProperty("webdriver.chrome.driver", "/Users/shamimchy/Documents/chromedriver");
//
//        webDriver = new ChromeDriver();

        webDriver.manage().timeouts().implicitlyWait(25, TimeUnit.SECONDS);
        webDriver.navigate().to(url);
        System.out.print(" Current URL /////  "+webDriver.getCurrentUrl());
        webDriver.manage().window().maximize();

    }


    @AfterMethod
    public void cleanUp(){
        webDriver.quit();
    }

    //Helping methods

    public void clickByCss(String locator){
        webDriver.findElement(By.cssSelector(locator)).click();
    }

    public void clickByXpath(String locator){
        webDriver.findElement(By.xpath(locator)).click();
    }

    public void typeByCss(String locator, String value) {
        webDriver.findElement(By.cssSelector(locator)).sendKeys(Keys.CLEAR, value);
    }
    public void typeByCssThenEnter(String locator, String value){
        webDriver.findElement(By.cssSelector(locator)).sendKeys(Keys.CLEAR, value, Keys.ENTER);

    }

    public List<String> getTextFromWebElements(String locator){
        List<WebElement> element = new ArrayList<WebElement>();

        List<String> text = new ArrayList<String>();
        element=webDriver.findElements(By.cssSelector(locator));
        for(WebElement web:element){
            text.add(web.getText());
        }
        return  text;

    }


    public void clearInput(String locator){
        webDriver.findElement(By.cssSelector(locator)).clear();
    }
    public void keysInput(String locator){
        webDriver.findElement(By.cssSelector(locator)).sendKeys(Keys.ENTER);
    }
    public void typeByXpath(String locator){
        webDriver.findElement(By.xpath(locator)).click();
    }

    public void clickById(String locator){
        webDriver.findElement(By.id(locator)).click();
    }
    public void clickByName(String name){
        webDriver.findElement(By.name(name)).click();
    }
    public void navigateBack(){
        webDriver.navigate().back();
    }
    public void navigateForward(){
        webDriver.navigate().forward();
    }
    public String getTextByCss(String locator){
        String st = webDriver.findElement(By.cssSelector(locator)).getText();
        return st;
    }
    public String getTextByXpath(String locator){
        String st = webDriver.findElement(By.xpath(locator)).getText();
        return  st;
    }
    public String getTextByName(String locator){
        String st = webDriver.findElement(By.name(locator)).getText();
        return st;
    }
    public WebElement getWebElementByCss(String locator){
        return webDriver.findElement(By.cssSelector(locator));
    }

}