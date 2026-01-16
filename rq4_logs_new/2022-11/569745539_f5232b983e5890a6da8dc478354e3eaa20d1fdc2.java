package pages;

	import org.openqa.selenium.By;
	import org.openqa.selenium.WebDriver;
	import org.openqa.selenium.WebElement;
	import org.openqa.selenium.chrome.ChromeDriver;

	public class FrameOnTestingBaba_Page {

		  public static void main(String[] args) throws InterruptedException {
			System.setProperty("webdriver.chrome.driver","" +"drivers\\chromedriver.exe");
	        WebDriver driver=new ChromeDriver();
	        driver.get("https://www.testingbaba.com/old/newdemo.html");
	     //   driver.manage().window().maximize();

	        driver.findElement(By.xpath("//*[@data-target=\"#alerts\"]")).click();
	        Thread.sleep(1000);
	        driver.findElement(By.xpath("//*[text()='frames']")).click();
	        
	        
	        
		  WebElement frame1 =driver.findElement(By.xpath("//*[@src=\"Framelink.html\"]"));
		  driver.switchTo().frame(frame1);
		  
		  WebElement frame1text =driver.findElement(By.xpath("//*[text()='This is a sample page']"));
		  String text1=frame1text.getText();
		  System.out.println(text1);
		 
		  driver.switchTo().defaultContent();
		 
		  
		  WebElement frame2 =driver.findElement(By.xpath("//*[@src=\"Framelink.html\"]"));
		  driver.switchTo().frame(frame2);
		  
		  WebElement frame2text =driver.findElement(By.xpath("//*[text()='This is a sample page']"));
		  String text2=frame2text.getText();
		  System.out.println(text2);
		 
		  
		  
		  }
}	        