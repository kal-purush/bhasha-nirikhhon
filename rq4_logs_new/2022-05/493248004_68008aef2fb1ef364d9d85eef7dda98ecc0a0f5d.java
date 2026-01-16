package managers;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.edge.EdgeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.opera.OperaDriver;
import org.openqa.selenium.opera.OperaOptions;
import org.openqa.selenium.safari.SafariDriver;
import org.openqa.selenium.safari.SafariOptions;
import utilities.ConfigFileReader;
import utilities.applicationUtilities.Parsers;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DriverManager {

    private static WebDriver sauceDemoDriver;

    public static ConfigFileReader configFileReader = new ConfigFileReader();
    public static Properties properties = configFileReader.getPropValues("configuration.properties");
    public static Properties externalDataProperties = configFileReader.getPropValues("externalDataConfiguration.properties");
    public static String absolutePath = "";
    public static Parsers parsers = new Parsers();
    public static Map<String, String> map = new HashMap<>();

    public static Logger LOGGER = LogManager.getLogger(DriverManager.class);

    public static String downloadLocation = "";

    private static WebDriver setupDriver() {
        LOGGER.info("Set up local driver");
        WebDriver driver = null;
        String browser = properties.getProperty("browser");

        switch (browser) {
            case "chrome":
                Map<String, Object> prefs = new HashMap<String, Object>();
                downloadLocation = System.getProperty("user.home") + File.separator + "Downloads" + File.separator + "qs_tool_reports";
                if(!new File(downloadLocation).exists()) { new File(downloadLocation).mkdir(); }
                prefs.put("download.default_directory", downloadLocation);
                prefs.put("profile.default_content_settings.popups", 0);
                prefs.put("profile.content_settings.exceptions.automatic_downloads.*.setting", 1 );

                WebDriverManager.chromedriver().setup();
                ChromeOptions chromeOptions = new ChromeOptions();
                chromeOptions.setExperimentalOption("prefs", prefs);

                driver = new ChromeDriver(chromeOptions);
                driver.manage().window().maximize();
                driver.manage().timeouts().implicitlyWait(Integer.parseInt(properties.getProperty("implicitWait")), TimeUnit.SECONDS);
                break;

            case "edge":
                WebDriverManager.edgedriver().setup();
                EdgeOptions edgeOptions = new EdgeOptions();
                driver = new EdgeDriver(edgeOptions);

                driver.manage().window().maximize();
                driver.manage().timeouts().implicitlyWait(Integer.parseInt(properties.getProperty("implicitWait")), TimeUnit.SECONDS);
                break;

            case "firefox":
                WebDriverManager.firefoxdriver().setup();
                FirefoxOptions firefoxOptions = new FirefoxOptions();

                driver = new FirefoxDriver(firefoxOptions);
                driver.manage().window().maximize();
                driver.manage().timeouts().implicitlyWait(Integer.parseInt(properties.getProperty("implicitWait")), TimeUnit.SECONDS);
                break;

            case "opera":
                WebDriverManager.operadriver().setup();
                OperaOptions operaOptions = new OperaOptions();
                driver = new OperaDriver(operaOptions);
                driver.manage().window().maximize();
                driver.manage().timeouts().implicitlyWait(Integer.parseInt(properties.getProperty("implicitWait")), TimeUnit.SECONDS);
                break;

            case "safari":
                WebDriverManager.safaridriver().setup();
                SafariOptions safariOptions = new SafariOptions();
                driver = new SafariDriver(safariOptions);
                driver.manage().window().maximize();
                driver.manage().timeouts().implicitlyWait(Integer.parseInt(properties.getProperty("implicitWait")), TimeUnit.SECONDS);
                break;
        }

        return driver;
    }

    public static WebDriver getSauceDemoDriver() {
        sauceDemoDriver = setupDriver();
        return sauceDemoDriver;
    }

    public static String getRunLocation() {
        String runOn = System.getProperty("runOn");
        if (runOn == null) {
            LOGGER.info("No run location is defined in System Property. Reading from config file");
            runOn = properties.getProperty("runOn");
        }
        return runOn;
    }

    public static void killDriverProcess(){
        if(sauceDemoDriver != null){
            sauceDemoDriver.close();
            sauceDemoDriver.quit();
        }
    }


    public static Map<String, String> populateMap() throws Exception{
        absolutePath = DriverManager.properties.getProperty("dev_test_data");
        parsers = new Parsers();
        map = parsers.jsonToHashMap(DriverManager.externalDataProperties.getProperty("miscellaneous_data_file_name"));
        return map;
    }
}
