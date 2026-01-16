package Demo;



import java.util.concurrent.TimeUnit;



import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;




public class wait_implicit
{
WebDriver driver;
String projectPath = System.getProperty("user.dir");
@BeforeClass

public void setup()
{
System.setProperty("webdriver.chrome.driver", projectPath+"/Driver/chromedriver.exe");
driver = new ChromeDriver();
}

@Test
public void launch_app() throws Exception
{
driver.get("https://pega-86-demo.iqzsystems.io/prweb/app/default/b0aUC_g2NnC90uEC3dhEbKXJdQTMkXeP*/!STANDARD");
driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
}

@Test (dependsOnMethods = "launch_app")
public void enter_user() throws Exception
{
WebElement username = driver.findElement(By.xpath("//input[@id='txtUserID']"));
username.sendKeys("tcapron");
}

@Test (dependsOnMethods = "enter_user")
public void enter_password() throws Exception
{

WebElement password = driver.findElement(By.xpath("//input[@id='txtPassword']"));
password.sendKeys("Password@123");
}

@Test (dependsOnMethods = "enter_password")
public void clicklogin() throws Exception
{

WebElement loginbutton = driver.findElement(By.xpath("//button[@id='sub']"));
loginbutton.click();
}

@Test (dependsOnMethods = "clicklogin")
public void get_title() throws Exception
{
System.out.println("Page title is : " + driver.getTitle());
Thread.sleep(2000);
}

@AfterClass
public void finish()
{
driver.quit();
System.out.println("Test is successful");
}
}

