package com.ss.ita.pages;

import org.openqa.selenium.WebDriver;

import static com.ss.ita.locators.HeaderLocators.ENGLISH_LANGUAGE_BUTTON;
import static com.ss.ita.locators.HeaderLocators.UKRAINE_LANGUAGE_BUTTON;

public class LanguageBar extends BasePage{

    public LanguageBar(WebDriver driver) {
        super(driver);
    }

    public void clickOnChangingLanguageButtons(){
        driver.findElement(ENGLISH_LANGUAGE_BUTTON.getPath()).click();
        driver.findElement(UKRAINE_LANGUAGE_BUTTON.getPath()).click();

    }

}