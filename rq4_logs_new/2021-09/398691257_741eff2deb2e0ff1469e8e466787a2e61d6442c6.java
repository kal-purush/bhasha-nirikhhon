package framework;

import org.openqa.selenium.chrome.ChromeDriver;

public class ChromeDriverManager extends DriverManager{
	public static final String WEB_DRIVER_SYSTEM_PROPERTY_CHROME = "webdriver.chrome.driver";
	public static final String chromeDriverPath = "resources\\chromedriver.exe";
	
	public ChromeDriverManager(){
		super();
		System.setProperty(ChromeDriverManager.WEB_DRIVER_SYSTEM_PROPERTY_CHROME, ChromeDriverManager.chromeDriverPath);
	}
	
	public void createDriver() {
		setDriver(new ChromeDriver());
	}
}