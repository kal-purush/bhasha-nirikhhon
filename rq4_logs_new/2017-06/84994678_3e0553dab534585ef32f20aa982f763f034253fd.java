import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class CalculadoraTest {
	
	private static WebDriver driver;
	
	@BeforeClass
	public static void onInit() throws InterruptedException{
		//System.setProperty("webdriver.gecko.driver", "C://_Dev//geckodriver.exe");
		System.setProperty("webdriver.chrome.driver", "C://_Dev//chromedriver.exe");
		driver = new ChromeDriver();
		driver.get("http://www.calculatoria.com");
		driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		// Fecha a barra de cookies
		driver.findElement(By.xpath("//*[@id=\"cbody\"]/div/button")).click();
	}
	
	@Before
	public void beforeTest(){
		driver.findElement(By.linkText("C")).click();
	}
	
	@Test
	public void vinteVezesQuatro() {
		driver.findElement(By.linkText("C")).click();
	    driver.findElement(By.linkText("2")).click();
	    driver.findElement(By.linkText("0")).click();
	    driver.findElement(By.linkText("×")).click();
	    driver.findElement(By.linkText("4")).click();
	    driver.findElement(By.linkText("=")).click();
	    assertEquals("80", driver.findElement(By.name("exprdisplay")).getAttribute("value"));
	}
	
	@Test
	public void cemDivididoPorTres(){
	    driver.findElement(By.linkText("1")).click();
	    driver.findElement(By.linkText("0")).click();
	    driver.findElement(By.linkText("0")).click();
	    driver.findElement(By.linkText("÷")).click();
	    driver.findElement(By.linkText("3")).click();
	    driver.findElement(By.linkText("=")).click();
	    assertEquals("33.333", driver.findElement(By.name("exprdisplay")).getAttribute("value"));
	}
	
	@Test
	public void cincoDivididoPorDoisSomadoComQuatro(){
		driver.findElement(By.linkText("5")).click();
	    driver.findElement(By.linkText("÷")).click();
	    driver.findElement(By.linkText("2")).click();
	    driver.findElement(By.linkText("=")).click();
	    assertEquals("2.5", driver.findElement(By.name("exprdisplay")).getAttribute("value"));
	    
	    driver.findElement(By.cssSelector("span.bdel")).click();
	    driver.findElement(By.cssSelector("span.bdel")).click();
	    driver.findElement(By.linkText("+")).click();
	    driver.findElement(By.linkText("4")).click();
	    driver.findElement(By.linkText("=")).click();
	    assertEquals("6", driver.findElement(By.name("exprdisplay")).getAttribute("value"));
	}
	
	@AfterClass
	public static void onExit(){
		driver.quit();
	}

}