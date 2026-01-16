import org.openqa.selenium.PageLoadStrategy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.time.Duration;

public class TestAssert {
    WebDriver driver;
    WebDriverWait wait;

    @BeforeMethod
    public void setup() {
        System.setProperty("webdriver.chrome.driver", "chromedriver.exe");
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--start-maximized");
        options.addArguments("--disable-infobars");

        options.setPageLoadStrategy(PageLoadStrategy.NORMAL);
//        options.setPageLoadTimeout(Duration.ofSeconds(30));

        driver = new ChromeDriver(options);
        wait = new WebDriverWait(driver, Duration.ofSeconds(30));
//        driver.get("https://creativemarket.com/");

    }

    @Test
    public void assertExamples(){
        SoftAssert softAssert = new SoftAssert();
        softAssert.assertTrue("Selenium".equals("selenium"), "2nd Soft assert failed");
        System.out.println("Soft Assert Pass");
        Assert.assertEquals("4","5", "Not Equals");
        System.out.println("Assert Fails");
    }
}