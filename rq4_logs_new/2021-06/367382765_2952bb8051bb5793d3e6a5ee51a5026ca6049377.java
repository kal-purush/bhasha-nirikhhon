package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class AlertsPage {

    private WebDriver driver;
    private By jsAlert = By.cssSelector("#content > div > ul > li:nth-child(1) > button");
    private By textJsAlert = By.id("result");
    private By jsConfirm = By.cssSelector("#content > div > ul > li:nth-child(2) > button");
    private By jsPrompt = By.cssSelector("#content > div > ul > li:nth-child(3) > button");

    public AlertsPage(WebDriver driver) {
        this.driver = driver;
    }

    public void acceptJsAlert(){
        driver.findElement(jsAlert).click();
        driver.switchTo().alert().accept();
    }

    public void acceptJsConfirm(){
        driver.findElement(jsConfirm).click();
        driver.switchTo().alert().accept();
    }

    public void cancelJsConfirm(){
        driver.findElement(jsConfirm).click();
        driver.switchTo().alert().dismiss();
    }

    public void acceptJsPrompt(){
        driver.findElement(jsPrompt).click();
        driver.switchTo().alert().sendKeys("Ivan");
        driver.switchTo().alert().accept();
    }

    public void cancelJsPrompt(){
        driver.findElement(jsPrompt).click();
        driver.switchTo().alert().dismiss();
    }

    public String textJsAlert(){
        return driver.findElement(textJsAlert).getText();
    }
}