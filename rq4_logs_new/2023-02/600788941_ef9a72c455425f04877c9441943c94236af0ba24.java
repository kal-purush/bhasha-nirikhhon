package com.qa.opencart.factory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.io.FileHandler;

public class Driverfactory {

	public WebDriver driver;
	public Properties prop;
	public OptionsManager OptionsManager;

	public static String highlight;

	public static ThreadLocal<WebDriver> tlDriver = new ThreadLocal<WebDriver>();
	
	public WebDriver intdriver(Properties prop) {

		String browsername = prop.getProperty("browser").trim();
		System.out.println("Launching Browser" + browsername);

		highlight = prop.getProperty("highlight");
		OptionsManager = new OptionsManager(prop);

		switch (browsername) {
		case "chrome":

//			System.setProperty("webDriver.chrome.driver",
//					"C:\\Users\\anmol\\eclipse-workspace\\LearningPOMSeries\\chromedriver.exe");
			//driver = new ChromeDriver(OptionsManager.getChrome());
			tlDriver.set(new ChromeDriver(OptionsManager.getChrome()));
			
			break;

		case "edge":

//			System.setProperty("webDriver.edge.driver",
//					"C:\\Users\\anmol\\eclipse-workspace\\LearningPOMSeries\\msedgedriver.exe");
			//driver = new EdgeDriver();
			tlDriver.set(new EdgeDriver());
			break;

		case "firefox":
//			System.setProperty("webdriver.gecko.driver",
//					"C:\\Users\\anmol\\eclipse-workspace\\LearningPOMSeries\\geckodriver.exe");
			//driver = new FirefoxDriver(OptionsManager.getfirefox());
			tlDriver.set(new FirefoxDriver(OptionsManager.getfirefox()));
			
			break;

		default:
			System.out.println(".....Enter the Correct Browser Name :" + browsername + "......");
			break;

		}
		getDriver().manage().deleteAllCookies();
		getDriver().manage().window().maximize();
		getDriver().get(prop.getProperty("url"));
		return getDriver();

	}

	public static WebDriver getDriver () {
		return tlDriver.get();
	}
	
	
	public Properties intprop() {

		prop = new Properties();
		try {
			FileInputStream ip = new FileInputStream("./src/test/resources/config/config.properties");
			try {
				prop.load(ip);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prop;

	}


public static String getscreenshot() {
File srcFile =	((TakesScreenshot)getDriver()).getScreenshotAs(OutputType.FILE);
String Path = System.getProperty("user.dir")+ "/screenshot/" + System.currentTimeMillis()+".png";
File destination =new File(Path);

try {
	FileHandler.copy(srcFile, destination);
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
return Path;

}



}