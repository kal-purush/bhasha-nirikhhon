import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;

public class TestNotSendsKey {
    WebDriver driver;
    WebDriverWait wait;
    Actions action;
    @BeforeMethod
    public void setup() {
        System.setProperty("webdriver.chrome.driver", "C:\\Users\\DevGraphics\\IdeaProjects\\InterviewPractice\\src\\chromedriver_win32\\chromedriver.exe");
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--start-maximized");
        options.addArguments("--disable-infobars");

        driver = new ChromeDriver(options);

        action = new Actions(driver);
        wait = new WebDriverWait(driver, Duration.ofSeconds(30));
        driver.get("https://www.google.com/");
    }

    @Test
    public void notUseOfSendsKey(){
        WebElement search = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("input.gLFyf")));
        JavascriptExecutor js = (JavascriptExecutor) driver;
        js.executeScript("arguments[0].value = 'Hello World'",search);
        WebElement searchButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div[class='FPdoLc lJ9FBc'] input.gNO89b")));
        searchButton.click();

        driver.quit();
    }
}