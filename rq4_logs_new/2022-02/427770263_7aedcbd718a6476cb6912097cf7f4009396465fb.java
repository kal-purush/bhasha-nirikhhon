package utils;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class DriverSingleton {
    private static DriverSingleton instance;

    private WebDriver webDriver;

    private DriverSingleton() {
        WebDriverManager.chromedriver().setup();
        this.webDriver = new ChromeDriver();
    }

    public static DriverSingleton getInstance() {
        if (instance == null) {
            instance = new DriverSingleton();
        }
        return instance;
    }

    public static WebDriver getDriver() {
        return getInstance().webDriver;
    }
}