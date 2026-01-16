package com.sample.test.demo.tests;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.Select;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DemoTest2 {
    WebDriver driver = new ChromeDriver();

    @Test
    public void tc1DemoTest1() {

        WebDriverManager.chromedriver().setup();
        driver.manage().window().maximize();
        driver.get("http://localhost:63342/SQEDemonstrationChallengeUIProject22/com.sample/files/index.html?_ijt=edt0gr7mkkeeloq2k6gbu68ui1");
        System.out.println("HELLO WORLD");

        //Verify title:
        driver.findElement(By.id("Pizza Order Form"));
        String expectedTitle = "Pizza Order Form";
        String actualTitle = driver.getTitle();
        Assert.assertEquals(actualTitle, expectedTitle);

        //Verify Pizza1 Dropdown
        Select pizza1Dropdown = new Select(driver.findElement(By.id("pizza1Pizza")));
        WebElement currentlySelectedOption = pizza1Dropdown.getFirstSelectedOption();
        String actualTextOfCurrentOption = currentlySelectedOption.getText();
        String expectedText = "Choose Pizza";
        Assert.assertEquals(actualTextOfCurrentOption, expectedText);

        //Verify Toppings1 Dropdown
        Select Toppings1Dropdown = new Select(driver.findElement(By.xpath("//div[@id='pizza1']//select[@class='toppings1")));
        WebElement toppingsSelected = Toppings1Dropdown.getFirstSelectedOption();
        String actualTextSelectedOption = toppingsSelected.getText();
        String expectedSelectedText = "Choose a Toppings 1";
        Assert.assertEquals(actualTextSelectedOption, expectedSelectedText);


        //Verify Toppings2 Dropdown
        Select Toppings2Dropdown = new Select(driver.findElement(By.xpath("//div[@id='pizza1']//select[@class='toppings2'")));
        WebElement topping2Selected = Toppings2Dropdown.getFirstSelectedOption();
        String actualTextOfCurrentOption2 = topping2Selected.getText();
        String expectedText2 = "Choose a Toppings 2";
        Assert.assertEquals(actualTextOfCurrentOption2, expectedText2);

        //Verify Quantity
       WebElement quantity = driver.findElement(By.id("pizza1Qty"));
       quantity.sendKeys("2");


    }
    @Test
    // Positive TC
    // input valid data
    public void tc2DemoTest2() throws InterruptedException {
        WebElement inputName = driver.findElement(By.id("name"));
        inputName.sendKeys("mariamReyad" + Keys.ARROW_DOWN);

        WebElement inputEmail = driver.findElement(By.id("email"));
        inputEmail.sendKeys("mariam.reyad@yahoo.com" + Keys.ARROW_DOWN);

        WebElement inputPhone = driver.findElement(By.id("phone"));
        inputPhone.sendKeys("202667895" + Keys.ARROW_DOWN);

        WebElement creditCard = driver.findElement(By.id("ccpayment"));
        creditCard.click();

        WebElement placeOrder = driver.findElement(By.id("placeOrder"));
        placeOrder.click();


        String expectedText = "Thank you for your order! TOTAL: 13.5 Small 6 Slices - no toppings";
        String actualText = placeOrder.getText();
        if (actualText.equals(expectedText)) {
            System.out.println("Order is placed");
        } else {
            System.out.println("Invalid Order");
        }

    }
        @Test
        // Negative TC
        // input invalid email and phone number
        public void tc2DemoTest3() {
            WebElement inputName= driver.findElement(By.id("name"));
            inputName.sendKeys("mariamReyad"+ Keys.ARROW_DOWN);

            WebElement inputEmail = driver.findElement(By.id("email"));
            inputEmail.sendKeys("mariam.reyad"+ Keys.ARROW_DOWN);

            WebElement inputPhone = driver.findElement(By.id("phone"));
            inputPhone.sendKeys("202"+ Keys.ARROW_DOWN);

            WebElement creditCard = driver.findElement(By.id("ccpayment"));
            creditCard.click();

            WebElement placeOrder = driver.findElement(By.id("placeOrder"));
            placeOrder.click();


            String expectedText = "Thank you for your order! TOTAL: 0";
            String actualText = placeOrder.getText();
            Assert.assertEquals(actualText, expectedText);
            }






        }


