package lib;

import io.appium.java_client.AppiumDriver;
import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.ios.IOSDriver;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.net.URL;

public class Platform {

    private static final String PLATFORM_IOS = "ios";
    private static final String PLATFORM_ANDROID = "android";
    private static final String APPIUM_URL = "http://127.0.0.1:4723/wd/hub";

    private static Platform instance;
    private Platform(){}

    public static Platform getInstance(){
        if(instance == null){
            instance = new Platform();
        }
        return instance;
    }

    public AppiumDriver getDriver() throws Exception{
    URL URL = new URL(APPIUM_URL);
    if(this.isAndroid()){
        return new AndroidDriver(URL, this.getAndroidDesiredCapabilities());
    } else if(this.isIOS()){
        return new IOSDriver(URL, this.getIOSDesiredCapabilities());
    }else {
        throw new Exception("Cannot detect type of the Driver. Platform value: " + this.getPlatformVar());
    }
    }

    public boolean isAndroid(){
        return isPlatform(PLATFORM_ANDROID);
    }

    public boolean isIOS(){
        return isPlatform(PLATFORM_IOS);
    }

    //Подстановка capabilities для андроида
    private DesiredCapabilities getAndroidDesiredCapabilities(){

        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("platformName", "Android");
        capabilities.setCapability("deviceName", "AndroidTestDevice");
        capabilities.setCapability("platformVersion", "8.0");
        capabilities.setCapability("automationName", "Appium");
        capabilities.setCapability("appPackage", "org.wikipedia");
        capabilities.setCapability("appActivity", ".main.MainActivity");
        capabilities.setCapability("app", "/Users/administrator/Desktop/course/JavaAppiumAutomation/apks/org.wikipedia.apk");

        return capabilities;
    }

    //Подстановка capabilities для айоса
    private DesiredCapabilities getIOSDesiredCapabilities() {

        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability("platformName", "iOS");
        capabilities.setCapability("deviceName", "iPhone 11");
        capabilities.setCapability("platformVersion", "13.4");
        capabilities.setCapability("app", "/Users/administrator/Desktop/course/JavaAppiumAutomation/apks/Wikipedia.app");

        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        capabilities.setCapability("newCommandTimeout", 120);

        return capabilities;
    }

    //Сравнение системной переменной с указанной
    private boolean isPlatform(String my_platform){
        String platform = this.getPlatformVar();
    return my_platform.equals(platform);
    }

    //Получение переменной PLATFORM
    private String getPlatformVar(){
        return System.getenv("PLATFORM");
    }
}