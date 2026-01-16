package org.example;

import io.appium.java_client.AppiumBy;
import org.openqa.selenium.WebElement;

import java.net.MalformedURLException;


public class AddCart extends Login {
    public void Addcart() throws MalformedURLException {

        appiumDriver();
        login();

        WebElement product = androidDriver.findElement(AppiumBy.xpath("(//android.widget.TextView[@text=\"ADD TO CART\"])[2]"));
        product.click();
        WebElement cart = androidDriver.findElement(AppiumBy.xpath("//android.view.ViewGroup[@content-desc=\"test-Cart\"]/android.view.ViewGroup/android.widget.ImageView"));
        cart.click();

    }
    public String addcartassert(){
        WebElement quantity = androidDriver.findElement(AppiumBy.xpath("(//android.widget.TextView[@text=\"1\"])[2]"));
        //System.out.println(Title);
        return quantity.getText();


    }

}