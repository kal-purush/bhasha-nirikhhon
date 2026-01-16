import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.Color;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;

public class TestFindColor {
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
        driver.get("https://www.indiamart.com/proddetail/desktop-computer-19104628655.html");
    }

    @Test
    public void findColor(){
        WebElement element = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div#glp_pg-1")));
        String color = element.getCssValue("background-color");
        System.out.println("RGBA color: "+color);
        String hexColor = Color.fromString(color).asHex();
        System.out.println("HexColor: "+hexColor);
        driver.quit();
    }
}