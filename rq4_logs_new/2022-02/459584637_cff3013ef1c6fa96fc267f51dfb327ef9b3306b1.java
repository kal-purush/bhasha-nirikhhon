package com.nextbase.pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

public class CalendarPage extends BasePage{


    @FindBy(xpath = "//*[@class=\"main-buttons-item-text-title\"][1]")
    public WebElement myCalendarTab;

    @FindBy(css = "#top_menu_id_calendar_menu_company_calendar > a")
    public WebElement companyCalendarTab;

    @FindBy(css = "#pagetitle")
    public WebElement pageTitle2;
}