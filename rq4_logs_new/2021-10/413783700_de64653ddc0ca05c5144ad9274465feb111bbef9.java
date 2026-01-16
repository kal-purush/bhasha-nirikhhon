package com.ss.ita.econews;

import com.ss.ita.elements.Button;
import com.ss.ita.elements.TextArea;
import com.ss.ita.pages.CreateNewsPage;
import com.ss.ita.pages.EcoNewsPage;
import com.ss.ita.pages.HomePage;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.ss.ita.data.UserSignInData.*;

public class CreateNewsTest extends TestRuner {
    @Test
    public void createNews() {
        CreateNewsPage createNewsPage = new HomePage(driver)
                .getHeader()
                .login(TEST_USER.getEmail(), TEST_USER.getPassword())
                .clickHomePageLink()
                .clickEcoNewsLink()
                .clickCreateNewsButton()
                .clickTagNewsButton();
        TextArea setTitleArea = new CreateNewsPage(driver).setTitleTextArea();
        TextArea setContentArea = new CreateNewsPage(driver).setContentArea();
        createNewsPage.clickPublishButton();
        EcoNewsPage clickFilterBy = new EcoNewsPage(driver).clickFilterByNews();
        String title = new EcoNewsPage(driver).getCreatedNewsTitle().getText();
        Assert.assertEquals(title, "Test News");
    }
}