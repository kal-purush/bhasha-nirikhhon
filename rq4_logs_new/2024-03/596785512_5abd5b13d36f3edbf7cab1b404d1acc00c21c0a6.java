package bsn.advantage.pageobjects;

import java.util.ArrayList;
import java.util.List;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class CartPage extends NavbarPage {

    private final By pageTitle = By.xpath("//h3[contains(text(),'SHOPPING CART')]");
    private final String cartProductStr = "#shoppingCart table tr.ng-scope";
    private final By cartProduct = By.cssSelector(cartProductStr);
    private final By cartProductName = By.cssSelector("td label.productName");
    private final By cartProductRemove = By.cssSelector("td span.actions a.remove");

    public CartPage(WebDriver driver) {
        super(driver);
    }

    public void verifyIsOnCartPage() {
        this.waitUntilPresent(pageTitle);
        log.info("Cart page is displayed: " + this.getTextFromElement(pageTitle));
    }

    public List<String> getProductsInCart() {
        List<WebElement> products = this.findElements(cartProduct);
        List<String> productNames = new ArrayList<>();
        for (WebElement product : products) {
            productNames.add(product.findElement(cartProductName).getText());
        }
        return productNames;
    }

    public void removeAllProductsFromCart() {
        List<WebElement> products = this.findElements(cartProduct);
        for (WebElement product : products) {
            String productName = product.findElement(cartProductName).getText().trim();
            this.clickElement(product.findElement(cartProductRemove));
            log.info("Product removed from cart: " + productName);
        }
        log.info("All products removed from cart");
    }

    public void removeProductFromCart(String productName) {
        List<WebElement> products = this.findElements(cartProduct);
        for (WebElement product : products) {
            if (product.findElement(cartProductName).getText().trim().equals(productName.toUpperCase())) {
                this.clickElement(product.findElement(cartProductRemove));
                log.info("Product removed from cart: " + productName);
                return;
            }
        }
        log.error("Product not found in cart: " + productName);
    }
}