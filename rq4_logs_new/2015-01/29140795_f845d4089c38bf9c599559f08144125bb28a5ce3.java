import org.junit.Assert;
import org.junit.Test;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class MyFirstTest {

    @Test
    public void startWebDriver(){

        WebDriver driver = new FirefoxDriver();
        driver.navigate().to("http://seleniumsimdddplified.com");

        Assert.assertTrue("title start with selenium simplified", driver.getTitle().startsWith("Selenium Simplified"));


        driver.close();
        try
        {
            Thread.sleep(5000);
            driver.quit();
        }
        catch(Exception e)
        {
        }
        driver.quit();

    }
}