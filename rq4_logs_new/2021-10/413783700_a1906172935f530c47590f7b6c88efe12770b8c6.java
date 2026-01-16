package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.data.UserSignInData;
import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.locators.CreateNewsPageLocators;
import com.ss.ita.greencity.ui.pages.CreateNewsPage;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.PreviewPage;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.Test;

public class CreateTestWithoutTags extends TestRuner {
    @Test
    public void createTestWithoutTag(){
        CreateNewsPage createNewsPage = new HomePage(driver).
                getHeader().
                login(UserSignInData.TEST_USER.getEmail(), UserSignInData.TEST_USER.getPassword())
                .clickHomePageLink()
                .clickEcoNewsLink()
                .clickCreateNewsButton()
                .setTitle("Test create news without tag")
                .setContent("Description for test create news without tag");
        new WebDriverWait(driver, 1).until(ExpectedConditions.not(ExpectedConditions
                .elementToBeClickable(CreateNewsPageLocators.PUBLISH_BUTTON.getPath())));
        createNewsPage.clickPreviewButton();
        new WebDriverWait(driver, 1).until(ExpectedConditions
                .invisibilityOfElementLocated(CreateNewsPageLocators.PREVIEW_CREATE_BUTTON.getPath()));
    }
}