package helpers;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.DesiredCapabilities;

public class WebDriverSingleton {

    private static WebDriver driver;

    private WebDriverSingleton() {
    }

    public static WebDriver initDriver(String browser, DesiredCapabilities caps) {

            switch (browser) {
                case "firefox": {
                    driver = new FirefoxDriver(caps);
                    break;
                }
                case "chrome": {
                    driver = new ChromeDriver(caps);
                    break;
                }
                case "ie": {
                    driver = new InternetExplorerDriver(caps);
                    break;
                }

                default: {
                    driver = new FirefoxDriver(caps);
                }
            }

        return driver;
    }
}