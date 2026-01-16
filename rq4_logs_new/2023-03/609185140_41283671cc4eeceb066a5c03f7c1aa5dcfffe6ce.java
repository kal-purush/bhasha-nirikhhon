import org.openqa.selenium.By;
import org.openqa.selenium.PageLoadStrategy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.ConfigurationNotInvokedException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.time.Duration;
import java.util.List;

public class TestCheckBrokenImages {
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
    public void findBrokenImage() throws IOException {
        //The Child Selector (>) selects only the direct children of an element.
        List<WebElement> images = wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector("div#prodMr img")));

        System.out.println("List of Child Elements");
        for (WebElement image : images) {
            String src = image.getAttribute("src");
            URL url = new URL(src);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.connect();
            int responseCode = connection.getResponseCode();
            if(responseCode != 200){
                System.out.println("Image is broken: "+ src);
            }else{
                System.out.println("Not broken"+ src);
            }
            connection.disconnect();
        }
        driver.quit();
    }
}