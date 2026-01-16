package seleniumProject;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class NewSeleniumProject {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// declaration and instantiation of objects/variables
		System.setProperty("webdriver.chrome.driver", "D://chromedriver_win32/chromedriver.exe");
		//WebDriver driver = new FirefoxDriver();
		//comment the above 2 lines and uncomment below 2 lines to use Chrome
		//System.setProperty("webdriver.chrome.driver","G:\\chromedriver.exe");
		WebDriver driver = new ChromeDriver();
    	
        String baseUrl = "https://www.google.com";
        String expectedTitle = "Google";
        String actualTitle = "";
        
        
        // launch Fire fox and direct it to the Base URL
        driver.get(baseUrl);
        driver.manage().window().maximize();
        // get the actual value of the title
        actualTitle = driver.getTitle();
        driver.manage().window().maximize();
        /*
         * compare the actual title of the page with the expected one and print
         * the result as "Passed" or "Failed"
         */
        if (actualTitle.contentEquals(expectedTitle)){
            System.out.println("Test Passed!"+actualTitle);
        } else {
            System.out.println("Test Failed");
        }
       //driver.findElement(By.id(""));
        //close Fire fox
        driver.close();
       
    }
}

