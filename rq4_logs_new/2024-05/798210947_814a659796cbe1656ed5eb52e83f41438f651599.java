package com.punnyajoshi.Ecommerce;

import com.punnyajoshi.Ecommerce.Pages.CartPage;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

import static org.testng.AssertJUnit.assertTrue;

public class CarPageTest {
    @Test
    public void testProductListedCorrectlyOnCartPage() {
        // Initialize the driver
        WebDriver driver = new ChromeDriver();

        // Navigate to the website
        driver.get("https://web-playground.ultralesson.com");

        // Click on the "Cart" button
        driver.findElement(By.xpath("//a[@href='/cart']")).click();

        // Create a CartPage object
        CartPage cartPage = new CartPage(driver);

        // Add products to the cart
        cartPage.addProductToCart("Product 1");
        cartPage.addProductToCart("Product 2");

        // Verify that products are displayed correctly
        assertTrue(cartPage.productsAreDisplayedCorrectly(), Boolean.parseBoolean("Products are not displayed correctly."));

        // Close the driver
        driver.quit();
    }
}