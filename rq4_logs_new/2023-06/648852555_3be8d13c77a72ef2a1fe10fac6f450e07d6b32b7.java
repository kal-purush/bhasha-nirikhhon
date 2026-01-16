package com.cydeo.tests.day04_findElements_checkboxes_radio;

import com.cydeo.utilities.WebDriverFactory;
import com.google.common.base.Verify;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class T1_xpatch_cssSelector_practices {

    public static void main(String[] args) {

//        TC #1: XPATH and cssSelector Practices
//        1. Open Chrome browser
        WebDriverManager.chromedriver().setup();
        WebDriver driver = WebDriverFactory.getDriver("chrome");
        driver.manage().window().maximize();

//        2. Go to https://practice.cydeo.com/forgot_password
        driver.get("https://practice.cydeo.com/forgot_password");

//        3. Locate all the WebElements on the page using XPATH and/or CSS
//        locator only (total of 6)
//        a. “Home” link
        WebElement homeCssLink = driver.findElement(By.cssSelector("a[class='nav-link']"));
        WebElement homeCssLink2 = driver.findElement(By.cssSelector("a[href='/']"));
        WebElement homeXPathLink3 = driver.findElement(By.xpath("//a[@class='nav-link']"));
        WebElement homeXPathLink4 = driver.findElement(By.xpath("//a[.='Home']"));

        String homeLinkVerify = "Home";
        String homeLinkActual1 = homeCssLink.getText();
        String homeLinkActual2 = homeCssLink2.getText();
        String homeLinkActual3 = homeXPathLink3.getText();
        String homeLinkActual4 = homeXPathLink4.getText();

        Boolean homeLinkIsDisplayed1 = homeCssLink.isDisplayed();
        Boolean homeLinkIsDisplayed2 = homeCssLink2.isDisplayed();
        Boolean homeLinkIsDisplayed3 = homeXPathLink3.isDisplayed();
        Boolean homeLinkIsDisplayed4 = homeXPathLink4.isDisplayed();

        if (homeLinkIsDisplayed1) {
            System.out.println("Home link display --> PASSED!!!");
        } else {
            System.out.println("Home link display --> FAILED!!!");
        }

        if (homeLinkIsDisplayed2) {
            System.out.println("Home link display --> PASSED!!!");
        } else {
            System.out.println("Home link display --> FAILED!!!");
        }

        if (homeLinkIsDisplayed3) {
            System.out.println("Home link display --> PASSED!!!");
        } else {
            System.out.println("Home link display --> FAILED!!!");
        }

        if (homeLinkIsDisplayed4) {
            System.out.println("Home link display --> PASSED!!!\n");
        } else {
            System.out.println("Home link display --> FAILED!!!");
        }

        if (homeLinkVerify.equals(homeLinkActual1)) {
            System.out.println("Home link verification --> PASSED!!!");
        } else {
            System.out.println("Home link verification --> FAILED!!!");
        }

        if (homeLinkVerify.equals(homeLinkActual2)) {
            System.out.println("Home link verification --> PASSED!!!");
        } else {
            System.out.println("Home link verification --> FAILED!!!");
        }

        if (homeLinkVerify.equals(homeLinkActual3)) {
            System.out.println("Home link verification --> PASSED!!!");
        } else {
            System.out.println("Home link verification --> FAILED!!!");
        }

        if (homeLinkVerify.equals(homeLinkActual4)) {
            System.out.println("Home link verification --> PASSED!!!\n");
        } else {
            System.out.println("Home link verification --> FAILED!!!\n");
        }

//        b. “Forgot password” header
        WebElement forgotPasswordCss = driver.findElement(By.cssSelector("div[class='example']>h2"));
        WebElement forgotPasswordCss2 = driver.findElement(By.cssSelector("div[id='content']>div>h2"));
        WebElement forgotPasswordXpath = driver.findElement(By.xpath("//h2[.='Forgot Password']"));

        String passwordHeaderContains = "Forgot password";
        String passwordHeaderActual1 = forgotPasswordCss.getText();
        String passwordHeaderActual2 = forgotPasswordCss2.getText();
        String passwordHeaderActual3 = forgotPasswordXpath.getText();

        if (passwordHeaderContains.equalsIgnoreCase(passwordHeaderActual1)) {
            System.out.println("Password header verification --> PASSED!!!");
        } else {
            System.out.println("Password header verification --> FAILED!!!");
        }


        if (passwordHeaderContains.equalsIgnoreCase(passwordHeaderActual2)) {
            System.out.println("Password header verification --> PASSED!!!");
        } else {
            System.out.println("Password header verification --> FAILED!!!");
        }

        if (passwordHeaderContains.equalsIgnoreCase(passwordHeaderActual3)) {
            System.out.println("Password header verification --> PASSED!!!\n");
        } else {
            System.out.println("Password header verification --> FAILED!!!\n");
        }

//        c. “E-mail” text
        WebElement eMailTextCss = driver.findElement(By.cssSelector("label[for='email']"));
        WebElement eMailTextCss2 = driver.findElement(By.cssSelector("div[class='large-6 small-12 columns']>label"));
        WebElement eMailTextXpath = driver.findElement(By.xpath("//label[.='E-mail']"));
        WebElement eMailTextXpath2 = driver.findElement(By.xpath("//label[@for='email']"));

        String containsEmail = "E-mail";
        String actualEmailText = eMailTextCss.getText();
        String actualEmailText2 = eMailTextCss2.getText();
        String actualEmailText3 = eMailTextXpath.getText();
        String actualEmailText4 = eMailTextXpath2.getText();

        if (actualEmailText.contains(containsEmail)) {
            System.out.println("Email text verification --> PASSED!!!");
        } else {
            System.out.println("Email text verification --> FAILED!!!");
        }

        if (actualEmailText2.contains(containsEmail)) {
            System.out.println("Email text verification --> PASSED!!!");
        } else {
            System.out.println("Email text verification --> FAILED!!!");
        }

        if (actualEmailText3.contains(containsEmail)) {
            System.out.println("Email text verification --> PASSED!!!");
        } else {
            System.out.println("Email text verification --> FAILED!!!");
        }

        if (actualEmailText4.contains(containsEmail)) {
            System.out.println("Email text verification --> PASSED!!!\n");
        } else {
            System.out.println("Email text verification --> FAILED!!!");
        }

//        d. E-mail input box
        WebElement inputBoxCss = driver.findElement(By.cssSelector("input[type='text']"));
        WebElement inputBoxCss2 = driver.findElement(By.cssSelector("input[name='email']"));
        WebElement inputBoxXpath = driver.findElement(By.xpath("//input[@type='text']"));
        WebElement inputBoxXpath2 = driver.findElement(By.xpath("//input[@pattern='[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,3}$']"));

        boolean inputBoxIsDisplayed1 = inputBoxCss.isDisplayed();
        boolean inputBoxIsDisplayed2 = inputBoxCss2.isDisplayed();
        boolean inputBoxIsDisplayed3 = inputBoxXpath.isDisplayed();
        boolean inputBoxIsDisplayed4 = inputBoxXpath2.isDisplayed();

        if (inputBoxIsDisplayed1) {
            System.out.println("Input box is displayed in the page --> PASSED!!!");
        } else {
            System.out.println("Input box is displayed in the page --> FAILED!!!");
        }

        if (inputBoxIsDisplayed2) {
            System.out.println("Input box is displayed in the page --> PASSED!!!");
        } else {
            System.out.println("Input box is displayed in the page --> FAILED!!!");
        }

        if (inputBoxIsDisplayed3) {
            System.out.println("Input box is displayed in the page --> PASSED!!!");
        } else {
            System.out.println("Input box is displayed in the page --> FAILED!!!");
        }

        if (inputBoxIsDisplayed4) {
            System.out.println("Input box is displayed in the page --> PASSED!!!\n");
        } else {
            System.out.println("Input box is displayed in the page --> FAILED!!!");
        }

//        e. “Retrieve password” button
        WebElement retrievePassButtonCss = driver.findElement(By.cssSelector("button[id='form_submit']"));
        WebElement retrievePassButtonCss2 = driver.findElement(By.cssSelector("button[class='radius']"));
        WebElement retrievePassButtonXpath = driver.findElement(By.xpath("//button[@type='submit']"));

        boolean retrieveButtonIsDisplayed1 = retrievePassButtonCss.isDisplayed();
        boolean retrieveButtonIsDisplayed2 = retrievePassButtonCss2.isDisplayed();
        boolean retrieveButtonIsDisplayed3 = retrievePassButtonXpath.isDisplayed();

        if (retrieveButtonIsDisplayed1) {
            System.out.println("Retrieve password button is displayed --> PASSED!!!");
        } else {
            System.out.println("Retrieve password button is displayed --> FAILED!!!");
        }

        if (retrieveButtonIsDisplayed2) {
            System.out.println("Retrieve password button is displayed --> PASSED!!!");
        } else {
            System.out.println("Retrieve password button is displayed --> FAILED!!!");
        }

        if (retrieveButtonIsDisplayed3) {
            System.out.println("Retrieve password button is displayed --> PASSED!!!\n");
        } else {
            System.out.println("Retrieve password button is displayed --> FAILED!!!");
        }

//        f. “Powered by Cydeo" text
        WebElement poweredByTextCss = driver.findElement(By.cssSelector(""));
        WebElement poweredByTextCss2 = driver.findElement(By.cssSelector(""));
        WebElement poweredByTextXpath = driver.findElement(By.xpath(""));
        WebElement poweredByTextXpath2 = driver.findElement(By.xpath(""));


//        4. Verify all web elements are displayed.
//        First solve the task with using cssSelector only. Try to create 2 different
//        cssSelector if possible
//        Then solve the task using XPATH only. Try to create 2 different
//        XPATH locator if possible

    }

}