package com.commens.methods;

import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.interactions.Actions;

public class PressKeys {

    //键盘操作
    // 操作 下键和回车键
    public static void pressKeyDownEnter(WebDriver driver) throws InterruptedException {

        // 初始化 actions类
        Actions action = new Actions(driver);

        //按 下键
        action.sendKeys(Keys.ARROW_DOWN).perform();
        Thread.sleep(1000);
        //按 回车
        action.sendKeys(Keys.ENTER).perform();

    }

    public static void pressKeyUpEnter(WebDriver driver) throws InterruptedException {

        // 初始化 actions类
        Actions action = new Actions(driver);

        //按 上键
        action.sendKeys(Keys.ARROW_UP).perform();
        Thread.sleep(1000);
        //按 回车
        action.sendKeys(Keys.ENTER).perform();

    }

    public static void pressKeyQuikDown(WebDriver driver) throws InterruptedException {

        // 初始化 actions类
        Actions action = new Actions(driver);

        //按 商品快捷向下
        action.keyDown(Keys.CONTROL).keyDown(Keys.ALT).sendKeys(Keys.PAGE_DOWN);

    }
}