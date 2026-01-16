package Selenium11;

import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Jsexecutor1 {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		System.setProperty("webdrive.chrome.driver", "C:\\Users\\offic\\OneDrive\\Desktop\\Chrome.exe");
		ChromeDriver driver = new ChromeDriver();
		driver.get("https://www.webdriveruniversity.com/");
		
		//System.out.println(driver.getTitle());
		
		//Getting the Title
		
//		JavascriptExecutor js = (JavascriptExecutor) driver;
//		String jsScript = "return document.title";
//		String aa1 = (String)js.executeScript(jsScript);
//		System.out.println(aa1);
//	
//			
//		// Getting the element
		
		JavascriptExecutor js1 =(JavascriptExecutor)driver;
		String jsScript2 = "return document.querySelector('#contact-us')";
		WebElement cc = (WebElement)js1.executeScript(jsScript2);
		System.out.println(cc.getText());
		

		
		// program 2
//		driver.get("https://www.w3schools.com/jsref/tryit.asp?filename=tryjsref_alert");
//		WebElement e =driver.findElement(By.cssSelector("#iframeResult"));
//		driver.switchTo().frame(e);
//		JavascriptExecutor js = (JavascriptExecutor) driver;
//		String jsScript3 = "myFunction()";
//		js.executeScript(jsScript3);
//		driver.switchTo().alert().accept();
		Thread.sleep(5000);
		driver.quit();
		
		
		
		
	}


	}
