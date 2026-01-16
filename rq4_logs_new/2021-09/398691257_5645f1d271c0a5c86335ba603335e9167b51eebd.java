package test.java.DemoQa.framework;

import org.openqa.selenium.chrome.ChromeDriver;

public class ChromeDriverManager extends{

	public class ChromeDriverManager extends DriverManager{
		private final String WEB_DRIVER_SYSTEM_PROPERTY_CHROME = "webdriver.chrome.driver";
		private final String chromeDriverPath = "resources\\chromedriver.exe";

		ChromeDriverManager(){
			System.setProperty(WEB_DRIVER_SYSTEM_PROPERTY_CHROME, chromeDriverPath);
		}

		public void createDriver() {
			setDriver(new ChromeDriver());
		}
	}