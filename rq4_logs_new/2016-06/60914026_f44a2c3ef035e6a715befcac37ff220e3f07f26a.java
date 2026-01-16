package Driver;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class WebDrivers
{
  private static WebDriver driver;

  public static void CreateDriver()
  {
    driver = new FirefoxDriver();
  }

   public static WebDriver GetDriver() throws Exception
   {
     if (driver == null)
     {
       throw new Exception("WebDriver has not been initialised!");
     }
    return driver;
    }

    public static void QuitDriver()
    {
      if (driver != null)
      {
       driver.quit();
       driver = null;
      }
     }
 }