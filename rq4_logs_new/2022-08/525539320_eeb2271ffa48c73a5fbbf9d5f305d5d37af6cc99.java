package com.gworks.ui.controller;

import com.gworks.ui.services.ChromeWebDriverService;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.openqa.selenium.WebDriver;

@AllArgsConstructor
@NoArgsConstructor
public class ChromeWebDriverController {

    private ChromeWebDriverService chromeService = new ChromeWebDriverService();

    public WebDriver openUrlOnChrome(String url){return this.chromeService.openUrlOnChrome(url);}
    public void closeChrome(){this.chromeService.closeChrome();}
}