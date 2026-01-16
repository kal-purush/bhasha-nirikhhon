package com.gworks.ui.controller;

import com.gworks.ui.services.FillFormService;
import org.openqa.selenium.WebDriver;

public class FillFormController {

    private final FillFormService fillFormService = new FillFormService();

    public void fillLoginPage(WebDriver webDriver, String testCase){this.fillFormService.fillLoginPage(webDriver, testCase);}
    public void selectCityInCityPage(WebDriver webDriver, String testCase){this.fillFormService.selectCityInCityPage(webDriver);}
    public void clickLoginButton(WebDriver webDriver){this.fillFormService.clickLoginButton(webDriver);}
}