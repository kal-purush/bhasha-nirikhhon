package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.econews.EcoNewsListItemComponent;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ViewBtnTest extends TestRuner {
    @Test
    public void changeViewToList() {

        EcoNewsListItemComponent ecoNewsItem = new HomePage(driver).
                getHeader()
                .clickEcoNewsLink()
                .clickChangeViewButton()
                .getNews()
                .get(0);
        Assert.assertEquals(ecoNewsItem.getNewsClass(), "list-view-li-active");
    }
}