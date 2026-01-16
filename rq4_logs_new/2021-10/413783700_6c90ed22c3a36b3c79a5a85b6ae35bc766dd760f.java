package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.NewsPage;
import org.testng.annotations.Test;

import static com.ss.ita.econews.ui.data.UserSignInData.VLAD_DMYTRIV;

public class CommentTest extends TestRuner {

    @Test
    public void comment() {
        new HomePage(driver)
                .getHeader()
                .login(VLAD_DMYTRIV.getEmail(), VLAD_DMYTRIV.getPassword());
        NewsPage newsPage = new HomePage(driver)
                .getHeader()
                .clickEcoNewsLink()
                .clickFffNewsButton()
                .setComment("comment")
                .clickComment();
    }
}