package com.gworks.ui.controller;

import com.gworks.ui.services.EdgeWebDriverService;
import org.openqa.selenium.WebDriver;

public class EdgeWebDriverController {

    private final EdgeWebDriverService edgeWebDriverService = new EdgeWebDriverService();

    public WebDriver openUrlOnEdge(String url){return this.edgeWebDriverService.openUrlOnEdge(url);}
}