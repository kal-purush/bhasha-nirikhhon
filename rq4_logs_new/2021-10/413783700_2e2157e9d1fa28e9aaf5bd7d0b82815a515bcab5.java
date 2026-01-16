package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.data.UserSignInData;
import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.econews.EcoNewsPage;
import com.ss.ita.greencity.ui.pages.news.NewsPage;
import org.testng.Assert;
import org.testng.annotations.Test;


import static com.ss.ita.greencity.ui.locators.NewsListCommentsLocators.ALL_COMMENTS_LIST;

public class DeleteCommentTest extends TestRuner {
    @Test
    public void deleteComment() {
        EcoNewsPage ecoNewsPage = new HomePage(driver)
                .getHeader()
                .login(UserSignInData.TEST_USER.getEmail(), UserSignInData.TEST_USER.getPassword())
                .clickHomePageLink()
                .clickEcoNewsLink();
        ecoNewsPage.getNews().get(0).click();
        int comments_before_test = driver.findElements(ALL_COMMENTS_LIST.getPath()).size();
        new NewsPage(driver)
                .setCommentText("Test comment for DeleteCommentTest")
                .clickCommentButton();
        new NewsPage(driver).deleteFirstComment();
        int comments_after_test = driver.findElements(ALL_COMMENTS_LIST.getPath()).size();
        Assert.assertEquals(comments_before_test, comments_after_test);
    }
}