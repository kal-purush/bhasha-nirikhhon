import org.openqa.selenium.By;
import org.openqa.selenium.PageLoadStrategy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Action;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.time.Duration;

public class TestActionClass {
    WebDriver driver;
    WebDriverWait wait;

    Actions action;
    @BeforeMethod
    public void setup() {
        System.setProperty("webdriver.chrome.driver", "chromedriver.exe");
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--start-maximized");
        options.addArguments("--disable-infobars");

        options.setPageLoadStrategy(PageLoadStrategy.NORMAL);
//        options.setPageLoadTimeout(Duration.ofSeconds(30));

        driver = new ChromeDriver(options);

        action = new Actions(driver);
        wait = new WebDriverWait(driver, Duration.ofSeconds(30));
        driver.get("https://creativemarket.com/");

    }

    @Test
    public void actionClass(){
        WebElement logo = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("a.upper-nav__logo")));
        WebElement outdoor = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("a[href='/search/outdoor']")));

//        action.doubleClick(logo).build().perform();
        action.contextClick(outdoor).build().perform();//For right click
//        action.moveToElement(outdoor).build().perform();
    }


}