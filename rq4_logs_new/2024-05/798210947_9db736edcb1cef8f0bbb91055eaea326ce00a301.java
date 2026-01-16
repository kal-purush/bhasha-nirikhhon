package com.punnyajoshi.Ecommerce.Pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class ProductDetailsPage {
    private WebDriver driver;
    private By productDetailsLocator = By.cssSelector("div.product-details");
    private By productNameLocator = By.cssSelector("h1.product-name");
    private By addToCartButtonLocator = By.cssSelector("button.add-to-cart");

    public ProductDetailsPage(WebDriver driver) {
        this.driver = driver;
    }

    public boolean isProductDetailsLoaded() {
        WebDriverWait wait = new WebDriverWait(driver, 100);
        return wait.until(ExpectedConditions.visibilityOfElementLocated(productDetailsLocator))!= null;
    }

    public String getProductName() {
        return driver.findElement(productNameLocator).getText();
    }

    public void addToCart() {
        driver.findElement(addToCartButtonLocator).click();
    }

}