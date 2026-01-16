package com.ss.ita.econews.ui.econews;

import com.ss.ita.econews.ui.runner.TestRuner;
import com.ss.ita.greencity.ui.pages.HomePage;
import com.ss.ita.greencity.ui.pages.econews.EcoNewsListItemComponent;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;


public class ItemViewTest extends TestRuner {
    @Test
    public void itemViewTest() {

        EcoNewsListItemComponent ecoNewsItem = new HomePage(driver).
                getHeader()
                .clickEcoNewsLink()
                .getNews()
                .get(0);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd, yyyy", Locale.US);
        boolean valid;
        try {
            LocalDate.parse((ecoNewsItem.getData().getText()), formatter);
            valid = true;
        } catch (DateTimeParseException e) {
            valid = false;
        }

        softAssert.assertTrue(ecoNewsItem.getImage().isDisplayed());
        softAssert.assertTrue(ecoNewsItem.getTag().isDisplayed());
        softAssert.assertTrue(ecoNewsItem.getTitle().isDisplayed());
        softAssert.assertEquals(ecoNewsItem.getTitle().getText(),"1635251747206");
        softAssert.assertTrue(ecoNewsItem.getContent().isDisplayed());
        softAssert.assertEquals(ecoNewsItem.getContent().getText(),"Description for test Create News");
        softAssert.assertTrue(valid);
        softAssert.assertAll();

    }

}