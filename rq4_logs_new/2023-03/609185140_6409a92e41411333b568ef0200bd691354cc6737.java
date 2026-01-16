import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WindowType;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Set;

public class TestWindowHandles {
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
    public void newWindow(){
        String mainWindow = driver.getWindowHandle();
        System.out.println("Main Window Id: "+mainWindow);
        driver.switchTo().newWindow(WindowType.WINDOW);
        driver.get("https://www.tradingview.com/");
        driver.switchTo().newWindow(WindowType.WINDOW);
        driver.get("https://www.forex.com/");
        driver.switchTo().newWindow(WindowType.WINDOW);
        driver.get("https://www.mi.com/");

        Set<String> windows = driver.getWindowHandles();
        for(String window : windows){
            if(window.equals(mainWindow)) {
                continue;
            }
            driver.switchTo().window(window);
            System.out.println("Window Names: "+window);
            driver.close();
        }
        driver.quit();
    }
}