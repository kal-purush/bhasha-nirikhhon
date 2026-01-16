package com.punnyajoshi.Ecommerce.CartTest;

import com.punnyajoshi.Ecommerce.CartPackage.Cart;
import com.punnyajoshi.Ecommerce.Pages.CartPage;
import com.punnyajoshi.Ecommerce.Pages.Product;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

import static org.testng.AssertJUnit.assertTrue;

public class CarPageTest {
    @Test
    public void testProductListedCorrectlyOnCartPage() {
        WebDriver driver = new ChromeDriver();
        driver.get("https://web-playground.ultralesson.com");
        CartPage cartPage = new CartPage(driver);
        cartPage.navigateToCartPage(); // Ensure navigating to the correct URL

        // Use the method to assert that product details are displayed correctly
        boolean areDisplayedCorrectly = cartPage.areProductsDisplayedCorrectly("Sweater", "L", 49.99, 1);
        assertTrue("Products are not displayed correctly.", areDisplayedCorrectly);

        driver.quit();
    }

    @Test
    public void testCorrectCartTotalPrice() {
        Cart cart = new Cart();
        cart.addProduct(new Product("Sweater", "L", 1, 49.99));
        cart.addProduct(new Product("Hat", "One size", 2, 19.99));

        double expectedTotal = 49.99 + 2 * 19.99; // Correct calculation of total price
        Assertions.assertEquals(expectedTotal, cart.getTotalPrice());
    }

}