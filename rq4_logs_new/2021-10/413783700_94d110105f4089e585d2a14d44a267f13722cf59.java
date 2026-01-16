package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.data.UserSignInData;
import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.elements.Button;
import com.ss.ita.greencity.ui.pages.HomePage;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EmptyReplyTest extends TestRuner {
    @Test
    public void emptyReplyTest(){
        Button postReplyButton = new HomePage(driver).getHeader()
                .login(UserSignInData.TEST_USER.getEmail(), UserSignInData.TEST_USER.getPassword())
                .clickHomePageLink()
                .clickEcoNewsLink()
                .getNews().get(0)
                .click()
                .clickFirstReply()
                .getPostReplyButton();
        Assert.assertFalse(postReplyButton.isEnabled());
    }
}