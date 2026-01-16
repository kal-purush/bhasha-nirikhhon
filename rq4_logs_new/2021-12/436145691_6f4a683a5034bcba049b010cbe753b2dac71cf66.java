package MavenProject.MavenProject;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Action;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class SocialNetwork {
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("webdriver.chrome.driver", "C:\\Users\\00005737\\Selenium\\chromedriver.exe");
		WebDriver driver=new ChromeDriver();
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		driver.get("http://elearningm1.upskills.in/");
		
		//maximize window
		driver.manage().window().maximize();
		
		//login
		driver.findElement(By.id("login")).sendKeys("roy");
		driver.findElement(By.id("password")).sendKeys("roy123");
		driver.findElement(By.id("form-login_submitAuth")).click();
		System.out.println("Logged in sucessfully");
		
		//social network
		driver.findElement(By.xpath("//*[@title='Social network']")).click();
		
		//message
		driver.findElement(By.xpath("//*[@title='Messages']")).click(); 
		
		//compose message
		driver.findElement(By.xpath("//*[@title='Compose message']")).click(); 
		driver.findElement(By.xpath("//*[@class='select2-search__field']")).sendKeys("raju");
		Thread.sleep(3000);
		
		List<WebElement> listOfElements=driver.findElements(By.xpath("//*[@id='select2-compose_message_users-results']/li"));
		for (WebElement webElement : listOfElements) {
			if(webElement.getText().trim().equals("Savitha Kanakaraju (savitha)")){
				webElement.click();
				break;
			}
		}
		driver.findElement(By.xpath("//*[@id='compose_message_title']")).sendKeys("Presentation");
		
		WebElement message = driver.findElement(By.xpath("//*[@class='cke_wysiwyg_frame cke_reset']"));
		driver.switchTo().frame(message);
		driver.findElement(By.xpath("//*[@class='cke_editable cke_editable_themed cke_contents_ltr cke_show_borders']")).sendKeys("My Presentation Report");
		
		driver.switchTo().defaultContent();
		driver.findElement(By.id("compose_message_title")).sendKeys("Presentation Report");
		
		driver.findElement(By.xpath("//*[@id='file-descrtiption']")).sendKeys("My Presentation Report");
		driver.findElement(By.xpath("//*[@id='compose_message_compose']")).click();
		
		//outbox
		driver.findElement(By.xpath("//*[@title='Outbox']")).click(); 
		
		//selectall
		driver.findElement(By.linkText("Select all")).click();
		
		//delete
		driver.findElement(By.xpath("//*[@class='btn btn-default dropdown-toggle']")).click();
		driver.findElement(By.linkText("Delete selected messages")).click();
		
		//accept alert
		//driver.switchTo().alert().accept();
		
		//dismiss alert
		driver.switchTo().alert().dismiss();

		//unselectall
		driver.findElement(By.linkText("Unselect all")).click();
		
	
		//social groups
		driver.findElement(By.xpath("//*[@title='Social network']")).click();
	
		//my groups
		driver.findElement(By.linkText("My groups")).click();
		
		//groups
		driver.findElement(By.linkText("rockers")).click();
		
		//see members
		driver.findElement(By.linkText("Members")).click();
		
		//create thread
		driver.findElement(By.xpath("//*[@title='Compose message']")).click();
		Thread.sleep(1000);
		WebElement txtTitle = driver.findElement(By.xpath("//*[@id='form_title']"));
		
		Actions builder = new Actions(driver);
		Action actions = builder
			.moveToElement(txtTitle) 
			.click() 
			.sendKeys(txtTitle, "Selenium") 
			.build();
			actions.perform();
			
		WebElement topicMessage = driver.findElement(By.xpath("//*[@class='cke_wysiwyg_frame cke_reset']"));
		driver.switchTo().frame(topicMessage);
		driver.findElement(By.xpath("//*[@class='cke_editable cke_editable_themed cke_contents_ltr cke_show_borders']")).sendKeys("Selenium Course");
		
		driver.switchTo().defaultContent();
		
		driver.findElement(By.xpath("//*[@id='form_submit']")).click();
		
		//social network
		driver.findElement(By.xpath("//*[@id=\"navbar\"]/ul[1]/li[5]/a")).click();
			
		//logout
		driver.findElement(By.cssSelector(".dropdown-toggle")).click();
		driver.findElement(By.id("logout_button")).click();
		System.out.println("Student logged out");
		
		driver.close();
		
	}
}