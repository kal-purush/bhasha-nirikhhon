package pageObjects;

import managers.PageObjectManager;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

import java.util.List;

public class SauceDemoCheckoutCompletePage extends PageObjectManager {


    @FindBy(className = "title")
    List<WebElement> title;

    public SauceDemoCheckoutCompletePage(WebDriver driver) { super(driver); }

    public boolean isOnCheckoutCompletePage(String page){
        return isElementDisplayed(title);
    }
}