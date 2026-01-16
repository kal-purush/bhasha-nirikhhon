package com.punnyajoshi.Ecommerce.Pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class ProductSelectionPage {
    private WebDriver driver;

    @FindBy(xpath = "//some-xpath")
    private WebElement productElement;

    public ProductSelectionPage(WebDriver driver) {
        this.driver = driver;
        PageFactory.initElements(driver, this);
    }

    public boolean isProductAvailable() {
        return productElement.isDisplayed();
    }

    public void selectProduct() {
        productElement.click();
    }

    public String getSelectedProductName() {
        return productElement.getText();
    }
}