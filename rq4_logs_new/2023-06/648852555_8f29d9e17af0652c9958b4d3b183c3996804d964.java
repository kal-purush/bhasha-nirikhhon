package com.cydeo.tests.day03_cssSelector_xpath;

import com.cydeo.utilities.WebDriverFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class T1_NextBaseCRM {

    public static void main(String[] args) {

        //        TC #2: NextBaseCRM, locators, getText(), getAttribute() practice
//        1- Open a chrome browser
        WebDriver driver = WebDriverFactory.getDriver("chrome");
        driver.manage().window().maximize();

//        2- Go to: https://login1.nextbasecrm.com/
        driver.get("https://login1.nextbasecrm.com/");

//        3- Verify “remember me” label text is as expected:
//        3.5 - Locate Label
//        Expected: Remember me on this computer
        WebElement rememberLabel = driver.findElement(By.tagName("label"));

        String expectedLabel = "Remember me on this computer";
        String actualLabel = rememberLabel.getText();

        if (expectedLabel.equals(actualLabel)) {
            System.out.println("Label verification --> PASSED!!!\n");
        } else {
            System.out.println("Label verification --> FAILED!!!\n");
        }

//        4- Verify “forgot password” link text is as expected:
//        Expected: Forgot your password?
        WebElement passwordText = driver.findElement(By.className("login-link-forgot-pass"));

        String expectedPassText = "FORGOT YOUR PASSWORD?";
        String actualPassText = passwordText.getText();

        if (expectedPassText.equals(actualPassText)) {
            System.out.println("Forgot password text verification --> PASSED!!!\n");
        } else {
            System.out.println("Forgot password text verification --> FAILED!!!" + "\nActual text is: " + actualPassText + "\n");
        }

//        5- Verify “forgot password” href attribute’s value contains expected:
//        Expected: forgot_password=yes
        String passHrefContains = "forgot_password=yes";
        String actualPassHreAtr = passwordText.getAttribute("href");

        if (actualPassHreAtr.contains(passHrefContains)) {
            System.out.println("Forgot password href attribute verification --> PASSES!!!\n");
        } else {
            System.out.println("Forgot password href attribute verification --> FAILED!!!" + "\nActual href attribute text:" + actualPassHreAtr);
        }

        driver.quit();
    }


}