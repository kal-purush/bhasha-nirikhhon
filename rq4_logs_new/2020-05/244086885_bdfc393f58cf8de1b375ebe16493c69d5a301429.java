package Pages;
import java.util.concurrent.*;
import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
public class Firsttestcase {
 private static WebDriver driver = null;
 public static void main (String[]args) {
	 driver = new ChromeDriver();
	 driver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS);
	 driver.get("https://ui.freecrm.com");
	 driver.navigate();
	 driver.navigate().to("https://seleniumjobs.com/job/qa-automation-engineer/");
	 Home_Page.lnk_MyAccount(driver).click();
	 Loginpage.WebElementtxtbx_UserName(driver).sendKeys("ch.radhika5@gmail.com");
	 Loginpage.WebElementtxtbx_Password(driver).sendKeys("Rs1517");
	 Loginpage.btn_Login(driver).click();
	 System.out.println("Login successfully");
	 Home_Page.lnk_Logout(driver).click();
	 driver.quit();
	 
 }
}