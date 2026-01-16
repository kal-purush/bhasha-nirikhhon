package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.data.UserSignInData;
import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.CreateNewsPage;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.econews.EcoNewsPage;
import com.ss.ita.greencity.ui.pages.econews.NewsPage;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.Test;
import static com.ss.ita.greencity.ui.locators.NewsLocators.COMMENTS_COUNT_LABEL;

public class PostCommentTest extends TestRuner {
    @Test
    public void postComment(){
        CreateNewsPage createNewsPage = new HomePage(driver)
                .getHeader()
                .login(UserSignInData.TEST_USER.getEmail(), UserSignInData.TEST_USER.getPassword())
                .clickHomePageLink()
                .clickEcoNewsLink()
                .clickCreateNewsButton()
                .clickTagNewsButton();
        createNewsPage.setTitleTextArea(System.currentTimeMillis() + "");
        createNewsPage.setContentArea();
        createNewsPage.clickPublishButton();

        EcoNewsPage ecoNewsPage = createNewsPage.getHeader().clickEcoNewsLink();

        NewsPage newsPage = ecoNewsPage.getNews().get(0).click();
        String defaultCommentsCount = newsPage.getCommentsCount();

        newsPage.setComment("some comment").clickComment();

        // waiting for comments label to update its count
        WebDriverWait wait = new WebDriverWait(driver, 3);
        wait.until(ExpectedConditions.not(
                ExpectedConditions.textToBePresentInElement(driver.findElement(COMMENTS_COUNT_LABEL.getPath()), defaultCommentsCount)));

        Assert.assertEquals(newsPage.getCommentsCount().replaceAll("\\D", ""), "1");
    }
}