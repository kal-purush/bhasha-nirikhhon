package pages.Home;

import com.aventstack.extentreports.ExtentTest;
import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import utils.CommonMethods;

import java.util.List;

public class Home extends PageBase {
    private HomeElements webElements;

    private Home(WebDriver driver){
        super(driver);
        this.webElements = new HomeElements();
    }

    public static Home of(WebDriver driver){
        return new Home(driver);
    }

    //region Web Elements
    private By ProductItems() { return By.cssSelector(webElements.PRODUCT_ITEMS); }
    //endregion

    //region Actions
    public Home chooseProductItem(){
        waitForElement(ProductItems());
        List<WebElement> products = driver.findElements(ProductItems());
        click(products.get(CommonMethods.randomNumber(0, products.size() - 1)));
        return this;
    }
    //endregion
}