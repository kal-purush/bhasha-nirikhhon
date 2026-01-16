package com.ss.ita.greencity.ui.elements;

import com.ss.ita.greencity.ui.locators.BaseLocator;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;

import java.io.File;

public class ImageArea extends BaseWebElement {

    public ImageArea(WebDriver driver, BaseLocator locator) {
        super(driver, locator);
    }

    public void dropFileToImgArea() {

        File file = new File("//src/test/resources/TestData/Tests.pdf");

        WebElement From=driver.findElement(By.xpath("//*[@id='credit2']/a"));
        WebElement To=driver.findElement(By.xpath("//*[@id='bank']/li"));

        Actions act=new Actions(driver);
        act.dragAndDrop(From, To);
    }
}