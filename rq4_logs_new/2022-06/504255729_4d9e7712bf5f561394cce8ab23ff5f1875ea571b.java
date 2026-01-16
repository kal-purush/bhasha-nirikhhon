package org.example;
import dev.failsafe.internal.util.Durations;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Discount {
    WebDriver driver;

    @BeforeClass
    public void setPathToWebDriver() {
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
    }

    @BeforeMethod
    public void setUp() {
        driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/main.py");
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
    }

    @Test
    public void logInAndAddingBook(){
        driver.get("https://www.sharelane.com/cgi-bin/register.py?page=1&zip_code=32454");
        driver.findElement(By.name("first_name")).sendKeys("dshgfh");
        driver.findElement(By.name("last_name")).sendKeys("dfjydhj");
        driver.findElement(By.name("email")).sendKeys("Lunekv@bk.ru");
        driver.findElement(By.name("password1")).sendKeys("243533");
        driver.findElement(By.name("password2")).sendKeys("243533");
        driver.findElement(By.cssSelector("input[value='Register']")).click();

        String email = driver.findElement(By.xpath("//td[text()='Email']/following::b")).getText();
        String password = driver.findElement(By.xpath("//td[text()='Password']/following::td")).getText();

        driver.findElement(By.cssSelector("img[src='../images/logo.jpg']")).click();

        driver.findElement(By.name("email")).sendKeys(email);
        driver.findElement(By.name("password")).sendKeys(password);
        driver.findElement(By.cssSelector("input[value='Login']")).click();

        driver.findElement(By.name("keyword")).sendKeys("Great Expectations");
        driver.findElement(By.cssSelector("input[value='Search']")).click();
        driver.findElement(By.cssSelector("img[src='../images/add_to_cart.gif']")).click();
        driver.findElement(By.cssSelector("a[href='./shopping_cart.py']")).click();
        driver.findElement(By.name("q")).clear();

    }
    @Test
    public void percentageTestForBuyingUpToTwentyBooks(){
        logInAndAddingBook();
        driver.findElement(By.name("q")).sendKeys("9");
        driver.findElement(By.cssSelector("input[value='Update']")).click();
        String discount = driver.findElement(By.xpath("//table//tr[6]//tr[2]//td[5]//b")).getText();
        String expectedDiskount = "0";
        Assert.assertEquals("Error discount should be correct",expectedDiskount,discount);
    }
    @Test
    public void percentageTestForBuyingUpToFiftyBooks(){
        logInAndAddingBook();
        driver.findElement(By.name("q")).sendKeys("50");
        driver.findElement(By.cssSelector("input[value='Update']")).click();
        String discount = driver.findElement(By.xpath("//table//tr[6]//tr[2]//td[5]//b")).getText();
        String expectedDiskount = "3";
        Assert.assertEquals("Error discount should be correct",expectedDiskount,discount);
    }
    @Test
    public void percentageTestForBuyingUpToHundredBooks(){
        logInAndAddingBook();
        driver.findElement(By.name("q")).sendKeys("100");
        driver.findElement(By.cssSelector("input[value='Update']")).click();
        String discount = driver.findElement(By.xpath("//table//tr[6]//tr[2]//td[5]//b")).getText();
        String expectedDiskount = "4";
        Assert.assertEquals("Error discount should be correct",expectedDiskount,discount);
    }
    @Test
    public void percentageTestForBuyingUpToFiveHundredBooks(){
        logInAndAddingBook();
        driver.findElement(By.name("q")).sendKeys("500");
        driver.findElement(By.cssSelector("input[value='Update']")).click();
        String discount = driver.findElement(By.xpath("//table//tr[6]//tr[2]//td[5]//b")).getText();
        String expectedDiskount = "5";
        Assert.assertEquals("Error discount should be correct",expectedDiskount,discount);
    }
    @Test
    public void percentageTestForBuyingUpToThousandBooks(){
        logInAndAddingBook();
        driver.findElement(By.name("q")).sendKeys("1000");
        driver.findElement(By.cssSelector("input[value='Update']")).click();
        String discount = driver.findElement(By.xpath("//table//tr[6]//tr[2]//td[5]//b")).getText();
        String expectedDiskount = "6";
        Assert.assertEquals("Error discount should be correct",expectedDiskount,discount);
    }
}
