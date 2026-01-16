package com.ss.ita.locators;

import org.openqa.selenium.By;


public enum CreateNewsLocators implements BaseLocator {
    CREATE_NEWS_BUTTON(By.id("create-button-text")),
    TITLE_AREA(By.xpath("//textarea[@placeholder='e.g. Coffee takeaway with 20% discount']")),
    NEWS_BUTTON(By.xpath("//button[normalize-space()='News']")),
    ADS_BUTTON(By.xpath("//button[normalize-space()='Ads']")),
    EVENTS_BUTTON(By.xpath("//button[normalize-space()='Events']")),
    INITIATIVES_BUTTON(By.xpath("//button[normalize-space()='Initiatives']")),
    EDUCATION_BUTTON(By.xpath("//button[normalize-space()='Education']")),
    SOURCE_AREA(By.xpath("//input[@placeholder='link to external source']")),
    CONTENT_AREA(By.name("main-area")),
    PUBLISH_BUTTON(By.cssSelector("button.submit"));

    private By path;

    CreateNewsLocators(By path) {
        this.path = path;
    }

    @Override
    public By getPath() {
        return path;
    }
}