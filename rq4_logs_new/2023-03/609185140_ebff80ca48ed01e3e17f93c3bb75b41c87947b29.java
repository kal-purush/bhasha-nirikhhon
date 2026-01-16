import org.openqa.selenium.By;
import org.openqa.selenium.PageLoadStrategy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;

public class TestXpathSelectors {
    WebDriver driver;
    WebDriverWait wait;

    @BeforeClass
    public void setup() {
        System.setProperty("webdriver.chrome.driver", "chromedriver.exe");
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--start-maximized");
        options.addArguments("--disable-infobars");
        options.setPageLoadStrategy(PageLoadStrategy.NORMAL);

        driver = new ChromeDriver(options);
        wait = new WebDriverWait(driver, Duration.ofSeconds(30));
        driver.get("https://creativemarket.com/");
    }

    @Test
    public void testAttributeValues() {
        WebElement element = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//li[@class='trending-category--fonts']")));
        System.out.println("Element Text: " + element.getText());
    }

    @Test
    public void testTextMethod() {
        WebElement element = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//*[text()='Display Fonts']")));
        System.out.println("Element Text: " + element.getText());

        WebElement elementh3 = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//h3[text()='Display Fonts']")));

        System.out.println("Element Text: " + elementh3.getText());
    }

    @Test
    public void testContainsMethod() {
        WebElement element = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//*[contains(@class, 'masthead__ti')]")));
        System.out.println("Element Text: " + element.getText());

        WebElement elementh3 = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//h1[contains(@class, 'masthead__ti')]")));

        System.out.println("Element Text: " + elementh3.getText());
    }

    @Test
    public void testSelectByPosition() {
        WebElement element = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//ul[@class='masthead__trending']/li[3]")));

        System.out.println(element.getText());
    }
    @Test
    public void testParentChildRelationship() {
        List<WebElement> elements = wait.until(ExpectedConditions
                .visibilityOfAllElementsLocatedBy(By
                        .xpath("//ul[@class='masthead__trending']/li")));

        for(WebElement element : elements){
            System.out.println(element.getText());
        }
    }
    @Test
    public void testAnd_Or_operators() {

        WebElement element = wait.until(ExpectedConditions
                .visibilityOfElementLocated(By
                        .xpath("//a[@href='/fonts/display' and @data-cta-track='Display Fonts']")));
        System.out.println("And Operator: "+element.getText());
    }

    @AfterClass
    public void testEnd() {
        driver.quit();
    }
}