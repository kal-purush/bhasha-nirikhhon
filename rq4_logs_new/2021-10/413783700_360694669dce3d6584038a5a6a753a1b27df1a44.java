package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.data.UserSignInData;
import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.news.NewsPage;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;


public class EditCommentTest extends TestRuner {

    /**
     * This test case verifies, that logged user can
     * edit its own comment on the 'News' page
     */
    @Test
    public void editComment() {
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);

        String primaryComment = System.currentTimeMillis() + "";
        String secondaryComment = primaryComment + "I";

        NewsPage page = new HomePage(driver)
                .getHeader()
                .login(UserSignInData.TEST_USER.getEmail(), UserSignInData.TEST_USER.getPassword())
                .getHeader().clickEcoNewsLink()
                .getNews()
                .get(0).click()
                .setCommentText(primaryComment)
                .clickCommentButton();

        // waiting for comment to appear
        WebDriverWait wait = new WebDriverWait(driver, 10);
        wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(
                String.format("//p[contains(text(),'%s')]", primaryComment)
                )));

        page
                .getComment()
                .get(0)
                .editComment(secondaryComment);

        String actual = driver
                .findElement(By.xpath(String.format("//p[contains(text(),'%s')]", secondaryComment)))
                .getText();

        Assert.assertEquals(actual, secondaryComment);
    }
}