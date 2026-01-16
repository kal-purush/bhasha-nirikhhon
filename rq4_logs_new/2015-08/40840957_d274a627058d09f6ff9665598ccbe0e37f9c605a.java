package hastingdirect;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class BaseMain {
	
	static final Logger log = Logger.getLogger("");
	static Properties prop_config = new Properties();
	protected static WebDriver driver = null;
	protected static Properties CONFIG = null;
	static Properties OR = null;
	static Boolean detailLogs = true;

	public void openBrowser() {
		if (driver == null) {
			CONFIG = new Properties();
			OR = new Properties();
			try {
				FileInputStream fs = new FileInputStream(
						System.getProperty("user.dir")
								+ "/src/test/resources/configuration/config.properties");
				CONFIG.load(fs);
				log.info("Loading config.properties");
				fs = new FileInputStream(System.getProperty("user.dir")
						+ "/src/test/resources/configuration/or.properties");
				log.info("Loading or.properties");
				OR.load(fs);
			} catch (Exception e) {
				log.error("Some problem has occured while loading config and or properties");
			}
		}
		log.info("Browser going to open :  " + CONFIG.getProperty("browser"));
		if (CONFIG.getProperty("browser").equals("Mozilla"))
			driver = new FirefoxDriver();
		else if (CONFIG.getProperty("browser").equals("IE")) {
			// driver = new InternetExplorerDriver();
		} else if (CONFIG.getProperty("browser").equals("Chrome")) {
			// driver = new ChromeDriver();
		}
		driver.get(CONFIG.getProperty("URL"));
		log.info("Navigating to  :  " + CONFIG.getProperty("URL"));
		driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
		driver.manage().window().maximize();
	}

	public void closeBrowser() {
		driver.close();
		driver.quit();
	}

	@SuppressWarnings("all")
	public static WebElement doAction(String ElementKey, String ElementValue) {
		try {
			boolean Presense;
			if (detailLogs.equals(true)) {
				log.info("do Action on " + ElementValue);
			}
			if (ElementKey.equalsIgnoreCase("xpath")) {
				WebElement myDynamicElement = (new WebDriverWait(driver, 30))
						.until(ExpectedConditions.presenceOfElementLocated(By
								.xpath(ElementValue)));
				Presense = myDynamicElement.isDisplayed();
				return driver.findElement(By.xpath(ElementValue));
			}
			if (ElementKey.equalsIgnoreCase("id")) {
				WebElement myDynamicElement = (new WebDriverWait(driver, 30))
						.until(ExpectedConditions.presenceOfElementLocated(By
								.id(ElementValue)));
				Presense = myDynamicElement.isDisplayed();
				return driver.findElement(By.id(ElementValue));

			}
			if (ElementKey.equalsIgnoreCase("link")) {
				WebElement myDynamicElement = (new WebDriverWait(driver, 30))
						.until(ExpectedConditions.presenceOfElementLocated(By
								.linkText(ElementValue)));
				Presense = myDynamicElement.isDisplayed();
				return driver.findElement(By.linkText(ElementValue));
			}

		} catch (AssertionError er) {
			log.error("Assertion error");
		} catch (NoSuchElementException er) {
			log.error("Action on element can be done reason may be LOCATOR MAY BE CHANGED ");
		}
		return driver.findElement(By.xpath(ElementValue));
	}

	public static int randomNumberGenerator() {
		Vector<Integer> vec = new Vector<Integer>();
		int Random_Value = 0;
		for (int i = 1; i <= 10000; i++)
			vec.add(i);
		Random rnd = new Random();
		while (vec.size() > 0) {
			int nextRnd = rnd.nextInt(vec.size());
			Random_Value = vec.remove(nextRnd);
		}
		return Random_Value;
	}

	public static String randomEmail() {
		return "Veinteractive" + randomNumberGenerator() + "@gmail.com";
	}

	public static String randomUserName() {
		return "QA" + randomNumberGenerator();
	}

}