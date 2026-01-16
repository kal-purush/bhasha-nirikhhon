package com.ss.ita.pages;

import org.openqa.selenium.WebDriver;

import static com.ss.ita.locators.PreviewLocators.*;

public class PreviewPage extends BasePage {
    public PreviewPage(WebDriver driver) {
        super(driver);
    }
    public String getTitle(){
       return driver.findElement(TITLE_FIELD.getPath()).getText();
    }

    public String getSource(){
        return driver.findElement(SOURCE_FIELD.getPath()).getText();
    }

    public String getContent(){
        return driver.findElement(CONTENT_FIELD.getPath()).getText();
    }
}