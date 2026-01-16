package core;

import io.appium.java_client.AppiumDriver;
import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.ios.IOSDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebElement;

import java.net.MalformedURLException;
import java.net.URL;

public class DriverFactory {
    private static String userBrowserstack = "carlossantanna_1uTv4f";
    private static String passwordBrowserstack = "zeQQJxMgywyjDXHGsWoX";
    private static String appBrowserstack = "bs://2c5091407dda44d43d4ff372b68586519cc971f5";
    private static AppiumDriver<RemoteWebElement> driver;
    public static void killDriver(){
        if (driver!= null){
            driver.quit ();
            driver= null;
        }
    }
    public static void restartDriver(){
        driver.resetApp();
    }


    public static AppiumDriver getDriver(String platform) throws MalformedURLException {
        if (driver == null) {
            if (platform == "ios") {
                return capsIos();
            }if (platform == "browserstackiOS") {
                return capsiOSrowserstack();
            }if (platform == "iPhone14ProMax") {
                return capsiOSrowserstackiPhone14ProMax();
            }if (platform == "iPhone13ProMax") {
                return capsiOSrowserstackiPhone13ProMax();
            }if (platform == "iPhone12ProMax") {
                return capsiOSrowserstackiPhone12ProMax();
            }if (platform == "iPhone11ProMax") {
                return capsiOSrowserstackiPhone11ProMax();
            }if (platform == "browserstackAndroid") {
                return capsAndroidBrowserstack();
            }
            if (platform == "android") {
                return capsAndroid();
            }
            if (platform == "iOSRealDevice") {
                return capsRealIos();
            }
        }
        return driver;
    }

    private static IOSDriver capsiOSrowserstack() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("browserstack.user", userBrowserstack);
        capabilities.setCapability("browserstack.key",passwordBrowserstack );
        capabilities.setCapability("platformVersion", "15.0");
        capabilities.setCapability("deviceName", "iPhone 13");
        capabilities.setCapability("app", appBrowserstack);


        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }private static IOSDriver capsiOSrowserstackiPhone14ProMax() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("realMobile", "true");
        capabilities.setCapability("browserstack.user", userBrowserstack);
        capabilities.setCapability("browserstack.key",passwordBrowserstack );
        capabilities.setCapability("platformVersion", "16.0");
        capabilities.setCapability("deviceName", "iPhone 14 Pro Max");
        capabilities.setCapability("app", appBrowserstack);


        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }private static IOSDriver capsiOSrowserstackiPhone13ProMax() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("realMobile", "true");
        capabilities.setCapability("browserstack.user", userBrowserstack);
        capabilities.setCapability("browserstack.key",passwordBrowserstack );
        capabilities.setCapability("platformVersion", "15.0");
        capabilities.setCapability("deviceName", "iPhone 13 Pro Max");
        capabilities.setCapability("app", appBrowserstack);


        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }private static IOSDriver capsiOSrowserstackiPhone12ProMax() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("realMobile", "true");
        capabilities.setCapability("browserstack.user", userBrowserstack);
        capabilities.setCapability("browserstack.key",passwordBrowserstack );
        capabilities.setCapability("platformVersion", "16.0");
        capabilities.setCapability("deviceName", "iPhone 12 Pro Max");
        capabilities.setCapability("app", appBrowserstack);


        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }private static IOSDriver capsiOSrowserstackiPhone11ProMax() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("browserstack.user", userBrowserstack);
        capabilities.setCapability("browserstack.key",passwordBrowserstack );
        capabilities.setCapability("app", appBrowserstack);
        capabilities.setCapability("realMobile", "true");
        capabilities.setCapability("deviceName", "iPhone 11 Pro Max");
        capabilities.setCapability("platformVersion", "16.0");


        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }
    public static AndroidDriver capsAndroidBrowserstack() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("browserstack.user", "carlosandrenoron_2rgUhz");
        capabilities.setCapability("browserstack.key", "soYp1G5VseKXeFGsGdEJ");
        capabilities.setCapability("platformVersion", "11");
        capabilities.setCapability("deviceName", "Google Pixel 3");
        capabilities.setCapability("app", "bs://4781e213330b4f6b64e58cd8d0c7845b72399c46");
        return (AndroidDriver) (driver = new AndroidDriver<RemoteWebElement>(new URL("http://hub.browserstack.com/wd/hub"), capabilities));
    }

    public static IOSDriver capsIos() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
//        capabilities.setCapability("app","/Users/premiersoft/Desktop/dede/LoginExample.app" );
//        capabilities.setCapability("app","/Users/premiersoft/Downloads/noteIt/dede.ipa" );
        capabilities.setCapability("bundleId", "me.bukovitz.NoteIt");
        capabilities.setCapability("platformName", "iOS");
        capabilities.setCapability("platformVersion", "15.0");
        capabilities.setCapability("deviceName", "Carlos 11");
        capabilities.setCapability("automationName", "XCUITest");
        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://localhost:4723/wd/hub"), capabilities));
    }
    public static IOSDriver capsRealIos() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("xcodesigningid","iPhone Delveloper" );
        capabilities.setCapability("xcodeOrgId","8TBGT267JT" );
        capabilities.setCapability("udid","00008030-000325483E11402E" );
        capabilities.setCapability("bundleId","me.bukovitz.NoteIt" );
        capabilities.setCapability("platformName","iOS" );
        capabilities.setCapability("platformVersion","15.5" );
        capabilities.setCapability("deviceName","iPhone SE - Carlos" );
        capabilities.setCapability("automationName","XCUITest" );
        return (IOSDriver) (driver = new IOSDriver<RemoteWebElement>(new URL("http://localhost:4723/wd/hub"), capabilities));
    }
    public static AndroidDriver capsAndroid() throws MalformedURLException {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("platformName", "Android");
        capabilities.setCapability("platformVersion", "11");
        capabilities.setCapability("deviceName", "Carlos 11");
        capabilities.setCapability("automationName", "uiautomator2");
        capabilities.setCapability("appPackage", "me.bukovitz.noteit");
        capabilities.setCapability("appActivity", "me.bukovitz.noteit.presentation.SplashActivity");
//        capabilities.setCapability("appActivity", "me.bukovitz.noteit.presentation.MainActivity");
//        capabilities.setCapability("unicodeKeyboard",true );
        return (AndroidDriver) (driver = new AndroidDriver<>(new URL("http://localhost:4723/wd/hub"), capabilities));
    }

}