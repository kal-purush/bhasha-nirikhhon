package com.punnyajoshi.Ecommerce.CartPackage;

import com.punnyajoshi.Ecommerce.Pages.CartPage;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CartCleanup {
    private WebDriver driver;
    private CartPage cartPage;

    public CartCleanup(WebDriver driver) {
        this.driver = driver;
        cartPage = new CartPage(driver);
    }

    public void cleanCart() {
        // Implement logic to remove the product from the cart
        // For example, if there is a "Remove" button for each product, you can find the button and click it
        cartPage.navigateToCartPage();
        List<WebElement> removeButtons = driver.findElements(By.xpath("//your-xpath-to-find-remove-buttons"));
        for (WebElement removeButton : removeButtons) {
            removeButton.click();
        }

        // Confirm the cart is empty
        assertTrue(cartPage.isCartEmpty(), "Cart is not empty after cleanup.");
    }

    public boolean isCartEmpty() {
        // Implement logic to check if the cart is empty
        // For example, if there is a message indicating the cart is empty, you can check for that message
        return cartPage.getCartMessage().contains("Your cart is empty");
    }
}