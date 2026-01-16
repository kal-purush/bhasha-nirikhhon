package pages;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;


public class CartPage extends BasePage {

    public static final By TITLE_NAME = By.cssSelector("[class=title]");
    public static final By PRODUCT_NAME = By.xpath("//a//div[@class='inventory_item_name']");
    public static final By PRODUCT_PRICE = By.xpath("//div[@class='inventory_item_price']");
    public static final By CONTINUE_SHOPPING_BUTTON = By.id("continue-shopping");

    public CartPage(WebDriver driver) {
        super(driver);
    }

    public void open() {
        driver.get("https://www.saucedemo.com/cart.html");
    }

    public String checkTheCartPage() {
        return driver.findElement(TITLE_NAME).getText();
    }

    public String checkTheNameItemInTheCart() {
        return driver.findElement(PRODUCT_NAME).getText();
    }

    public String checkThePriceItemInTheCart() {
        return driver.findElement(PRODUCT_PRICE).getText();
    }

    public void continueShopping(){
        driver.findElement(CONTINUE_SHOPPING_BUTTON).click();
    }
}

