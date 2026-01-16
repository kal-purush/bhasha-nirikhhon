package com.ss.ita.greencity.ui.locators;

import org.openqa.selenium.By;

public enum NewsListCommentsLocators implements BaseLocator {
    REPLY_FIRST_COMMENT_BUTTON(By.xpath("//div[contains(@class,'comment-body-wrapper wrapper-comment')][1]/descendant::button[@class='cta-btn reply']")),
    REPLY_TEXT_AREA(By.xpath("//textarea[@class='reply-width ng-pristine ng-invalid ng-touched']")),
    PUBLISH_REPLY_BUTTON(By.xpath("//button[@class='primary-global-button'][2]")),
    DELETE_FIRST_COMMENT_BUTTON(By.xpath("//span[contains(text(),' Delete ')][1]")),
    APPROVE_DELETING_COMMENT_BUTTON(By.xpath("//button[contains(text(),' Yes, delete ')]"));

    private final By path;

    NewsListCommentsLocators(By path) {
        this.path = path;
    }

    @Override
    public By getPath() {
        return path;
    }

}