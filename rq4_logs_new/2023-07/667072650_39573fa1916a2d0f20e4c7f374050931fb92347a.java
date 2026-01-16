package Androidapp;

import com.google.common.collect.ImmutableMap;
import io.appium.java_client.AppiumBy;
import org.openqa.selenium.JavascriptExecutor;
import org.testng.annotations.Test;

public class productorder extends Base{
    @Test
    public void verification()
    {
        //Login Process

        driver.findElement(AppiumBy.xpath("//android.widget.EditText[@index='0']")).click();
        driver.findElement(AppiumBy.xpath("//android.widget.EditText[@index='0']")).sendKeys("max5@gmail.com");
        driver.findElement(AppiumBy.xpath("//android.widget.EditText[@index='1']")).click();
        driver.findElement(AppiumBy.xpath("//android.widget.EditText[@index='1']")).sendKeys("Test@123");
        driver.hideKeyboard();
        driver.findElement(AppiumBy.xpath("//android.widget.Button[@content-desc='LOGIN']")).click();

        driver.findElement(AppiumBy.xpath("//android.view.View[@content-desc='Trousers']/android.widget.Button[2]")).click();
        driver.findElement(AppiumBy.xpath("//android.view.View[@content-desc='Trousers']/android.widget.Button[2]")).click();

        driver.findElement(AppiumBy.xpath("//android.view.View[@content-desc='Instant Pot']/android.widget.Button[2]")).click();

        ((JavascriptExecutor) driver).executeScript("mobile: scrollGesture", ImmutableMap.of(
                "left", 100, "top", 100, "width", 200, "height", 1783,
                "direction", "down",
                "percent",  10.0
        ));
        driver.findElement(AppiumBy.xpath("//android.view.View[@content-desc='Handheld Milk Frother']/android.widget.Button[2]")).click();


        driver.findElement(AppiumBy.xpath("android.widget.Button[@index='4']")).click();



    }
}