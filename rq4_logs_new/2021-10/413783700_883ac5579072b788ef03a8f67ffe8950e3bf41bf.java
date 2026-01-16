package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.econews.EcoNewsPage;
import com.ss.ita.greencity.ui.pages.HomePage;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FilterByNewsTest extends TestRuner {

    @Test
    public void filterByNews()  {
        EcoNewsPage ecoNewsPage = new HomePage(driver).getHeader().clickEcoNewsLink().clickFilterByNews();
        String text = ecoNewsPage.getFilterByNewsTag().getText();
        Assert.assertEquals(text, "NEWS");
    }
}