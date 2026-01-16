package lessons.lesson19.chromedriver;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DriverInitExample {

    private WebDriver driver;

    @BeforeTest
    public void initDriver() {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
    }

    @Test
    public void someTest() {
        driver.get("https://www.autodoc.de");
    }

    @AfterMethod
    public void after() {
        driver.close();
    }
}
