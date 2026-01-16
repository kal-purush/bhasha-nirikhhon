package YandexSearch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.*;
import org.openqa.selenium.firefox.FirefoxDriver;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.fail;


public class YandexSearch {

    private WebDriver driver;
    private String baseUrl;
    private String searchUrl;
    private StringBuffer verificationErrors = new StringBuffer();

    @Before
    public void setUp() throws Exception {
        driver = new FirefoxDriver();
        baseUrl = "http://yandex.ru";
        searchUrl = "ru.wikipedia.org";
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
    }

    @Test
    public void testFirst() throws Exception {

        driver.get(baseUrl);

        WebElement txtSearch = driver.findElement(By.xpath("//span[@class='input__box']/input "));
        WebElement btnSearch = driver.findElement(By.xpath("//table[@class='search__table suggest2-form__node']/ descendant::button[@type='submit']"));

        txtSearch.sendKeys("шкапа");
        btnSearch.click();

        WebElement firstLink = driver.findElement(By.xpath("//div[@class='serp-list']/ descendant::a[2]"));

        if (firstLink.getText().contains("searchUrl")){
            System.out.println("The first link is " + searchUrl);
            File screenshot = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
            BufferedImage fullImg = ImageIO.read(screenshot);
            ImageIO.write(fullImg, "png", screenshot);
            FileUtils.copyFile(screenshot, new File("C:\\Selenium\\YandexSearch\\Screenshot1.png"));

        }else{
            System.out.println("The first link is NOT " + searchUrl);
            File screenshot = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
            BufferedImage fullImg = ImageIO.read(screenshot);
            ImageIO.write(fullImg, "png", screenshot);
            FileUtils.copyFile(screenshot, new File("C:\\Selenium\\YandexSearch\\Screenshot2.png"));
        }
    }

    @After
    public void tearDown() throws Exception {
        driver.quit();
        String verificationErrorString = verificationErrors.toString();
        if (!"".equals(verificationErrorString)) {
            fail(verificationErrorString);
        }
    }
}