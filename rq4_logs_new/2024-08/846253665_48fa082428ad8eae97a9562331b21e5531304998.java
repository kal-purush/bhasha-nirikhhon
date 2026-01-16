package training2024.pages;

import java.time.Duration;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.*;
import io.github.bonigarcia.wdm.WebDriverManager;

public class InnerTrainingTest1 {    

    private WebDriver driver;

    @BeforeMethod
    public void setUp() {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();        
    }

    @Test
    public void navegamos() {
        
        WebDriverWait wait = new WebDriverWait(driver,Duration.ofSeconds(10));

        driver.get("https://www.freerangetesters.com/");

        wait.until(ExpectedConditions.urlToBe("https://www.freerangetesters.com/"));


        

    }

    @AfterMethod
    public void tearDown() {
      /*   if (driver != null) {
            driver.quit();
        }*/
    }

}