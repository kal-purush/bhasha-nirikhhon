package utilities;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;

public class DriverFactory {
	
	// This method returns a WebDriver object
	public static WebDriver open(String browserType) {
		
if (browserType.equalsIgnoreCase("Firefox")) {
			
			System.setProperty("webdriver.gecko.driver", "D:\\Automation with Java- DESKTOP ONLY\\SDET University -Selenium with Java\\Software\\geckodriver.exe");
			return new FirefoxDriver();
		}
		
		else if (browserType.equalsIgnoreCase("IE")) { 
			System.setProperty("webdriver.ie.driver", "D:\\Automation with Java- DESKTOP ONLY\\SDET University -Selenium with Java\\Software\\IEDriverServer.exe");
			return new InternetExplorerDriver();
			
		}

		else { 
			System.setProperty("webdriver.chrome.driver", "D:\\Google Drive\\CAREER\\AUTOMATION\\SDET University -Selenium with Java- Universal\\Software\\chromedriver.exe");
			return new ChromeDriver();	

		}
		
		
		
		
	}

}