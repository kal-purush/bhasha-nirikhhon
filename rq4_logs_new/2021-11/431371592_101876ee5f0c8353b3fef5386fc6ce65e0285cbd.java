import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HomeWorkSauceDemoTest extends BaseTest {
    String userNameInput = "user-name";
    String passwordInput = "password";
    String loginButton1 = "[value=Login]";

    @Test
    public void HomeWorkSauceDemo() {
        driver.get("https://www.saucedemo.com/");
        WebElement userNameField = driver.findElement(By.name(userNameInput));
        userNameField.sendKeys("problem_user");
        WebElement passwordField = driver.findElement(By.id(passwordInput));
        passwordField.sendKeys("secret_sauce");
        WebElement loginButton = driver.findElement(By.cssSelector(loginButton1));
        loginButton.click();

        WebElement buttonAddToCart = driver.findElement(By.xpath("//button[@id='add-to-cart-sauce-labs-bike-light']"));
        buttonAddToCart.click();
        WebElement shoppingCart = driver.findElement(By.id("shopping_cart_container"));
        shoppingCart.click();
        WebElement productName = driver.findElement(By.xpath("//a//div[@class='inventory_item_name']"));
        WebElement productPrice = driver.findElement(By.xpath("//div[@class='inventory_item_price']"));

        String actualResultProductName = productName.getText();
        String actualResultProductPrice = productPrice.getText();

        Assert.assertEquals(actualResultProductName, "Sauce Labs Bike Light", "The name of the item in the cart does not match the name of the item in the catalog");
        Assert.assertEquals(actualResultProductPrice, "$9.99", "The price of the item in the cart does not match the price of the item in the catalog");
    }
}