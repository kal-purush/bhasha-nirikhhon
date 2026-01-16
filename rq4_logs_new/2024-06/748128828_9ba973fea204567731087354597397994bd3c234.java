package common;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.safari.SafariDriver;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;
import java.io.IOException;
import java.time.Duration;


public class Browser {
    private static WebDriver driver;
    private static int MAXIMUM_TIME_OUT_IN_SECONDS = 40;
    public static WebDriverWait wait;

    public static void open(String browser) {
        switch (browser) {
            case "chrome": {
                ChromeOptions chromeOptions = new ChromeOptions();
                chromeOptions.addArguments("--remote-allow-origins=+");
                driver = new ChromeDriver(chromeOptions);
                break;
            }
            case "firefox": {
                driver = new FirefoxDriver();
                break;
            }
            case "edge": {
                driver = new EdgeDriver();
                break;
            }
            case "safari": {
                driver = new SafariDriver();
                break;
            }
        }
        wait = new WebDriverWait(driver, Duration.ofSeconds(MAXIMUM_TIME_OUT_IN_SECONDS));
    }

    public static WebDriver getDriver(){
        return driver;
    }

    //viết hàm in kết quả
    public static void captureScreen(String fileName){
        File file = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
        File DestFile = new File("./target/screenshot/"
                +fileName
                + "-"
                +System.currentTimeMillis()+".png");
        try {
            FileUtils.copyFile(file, DestFile);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public static void close(){
        if (driver!=null){
            driver.quit();
        }
    }

}