package baseUtil;
import java.time.Duration;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import pages.HomePage;

public class BaseClass {
	//(Remember we write  String name or int age
	//Samething WebDriver driver---> driver is of WebDriver type. This driver object will 
	//do most of the things to get all the properties from Webdriver.)
	//The reason of declaring like this is for resubility so that we can use multiple times with multiple browsers if needed.

	// Why default type is not ok for below 2 line?
		// because different package accessibility is not possible for default type
	public WebDriver driver;
	public HomePage homePage;

	@BeforeMethod
	public void setUp() {
		
		//Don’t make spelling mistake --> Below all lower case--> webdriver.chrome.driver
		System.setProperty("webdriver.chrome.driver","C:\\Users\\ifzal\\eclipse-workspace\\com.enthrallit\\driver\\chromedriver.exe");
		// We instantiated the driver here
		driver = new ChromeDriver();
		// maximize method is used to maximize the window ---> mostly used
		driver.manage().window().maximize();

		// HTTP cookies are small blocks of data created by a web server while a user is
		// browsing a website
		// deleteAllCookies do delete the cookies
		driver.manage().deleteAllCookies();
		// get() is used to access the url
		driver.get("https://enthrallit.com/");
		// PageLoadTimeout is used for wait to load the page for certain amount of time
		driver.manage().timeouts().pageLoadTimeout(Duration.ofSeconds(20));
		// Implicitly wait is used to wait for each web element
		driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(20));
		homePage = new HomePage(driver);// why driver? because its parameterized constructor and driver will access all
										// methods.
	}

	@AfterMethod
	public void tearUp() {// name of the method
		driver.quit(); // will close the window
	}
}