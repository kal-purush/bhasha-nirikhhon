package com.ss.ita.greencity.ui.pages.econews;

import com.ss.ita.greencity.ui.locators.EcoNewsListLocators;
import com.ss.ita.greencity.ui.pages.BasePage;
import org.openqa.selenium.WebDriver;

import java.util.LinkedList;
import java.util.List;

public class EcoNewsListComponent extends BasePage {
    private List<EcoNewsListItemComponent> news;

    public EcoNewsListComponent(WebDriver driver) {
        super(driver);
        init();
    }

    public List<EcoNewsListItemComponent> init() {
        if (news == null) {
            news = new LinkedList<>();
            driver.findElements(EcoNewsListLocators.ITEMS.getPath())
                    .forEach(webElement -> news.add(new EcoNewsListItemComponent(driver, webElement)));
        }
        return news;
    }

    public EcoNewsListItemComponent get(int i) {
        return news.get(i);
    }
}