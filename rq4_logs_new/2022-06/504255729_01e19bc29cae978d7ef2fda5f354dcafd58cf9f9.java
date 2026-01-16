package org.example;
import org.junit.Assert;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class SingUpTest {
    @Test
    public void zipCodeShouldBeValid1(){
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/register.py");
        driver.findElement(By.name("zip_code")).sendKeys("12345");
        driver.findElement(By.cssSelector("input[value='Continue']")).click();
        Assert.assertTrue(driver.findElement(By.cssSelector("input[value='Register']")).isDisplayed());
    }
    @Test
    public void zipCodeShouldBeInvalid1(){
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/register.py");
        driver.findElement(By.name("zip_code")).sendKeys("1234567");
        driver.findElement(By.cssSelector("input[value='Continue']")).click();
        Assert.assertTrue(driver.findElement(By.cssSelector("input[value='Register']")).isDisplayed());
    }
    @Test
    public void zipCodeShouldBeInvalid2(){
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/register.py");
        driver.findElement(By.name("zip_code")).sendKeys("");
        driver.findElement(By.cssSelector("input[value='Continue']")).click();
        Assert.assertTrue(driver.findElement(By.cssSelector("span[class='error_message']")).isDisplayed());
    }
    @Test
    public void allFieldShouldBeValid(){
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/register.py?page=1&zip_code=234534");
        driver.findElement(By.name("first_name")).sendKeys("dshgfh");
        driver.findElement(By.name("last_name")).sendKeys("dfjydhj");
        driver.findElement(By.name("email")).sendKeys("Lunekv@bk.ru");
        driver.findElement(By.name("password1")).sendKeys("243533");
        driver.findElement(By.name("password2")).sendKeys("243533");
        driver.findElement(By.cssSelector("input[value='Register']")).click();
        Assert.assertTrue(driver.findElement(By.cssSelector("span[class='confirmation_message']")).isDisplayed());
    }
    @Test
    public void firstNameShouldBeValid(){
        System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get("https://www.sharelane.com/cgi-bin/register.py?page=1&zip_code=234534");
        driver.findElement(By.name("first_name")).sendKeys("апври");
        driver.findElement(By.name("last_name")).sendKeys("dfjydhj");
        driver.findElement(By.name("email")).sendKeys("Lunekv@bk.ru");
        driver.findElement(By.name("password1")).sendKeys("243533");
        driver.findElement(By.name("password2")).sendKeys("243533");
        driver.findElement(By.cssSelector("input[value='Register']")).click();
        Assert.assertTrue(driver.findElement(By.cssSelector("span[class='error_message']")).isDisplayed());
    }


}