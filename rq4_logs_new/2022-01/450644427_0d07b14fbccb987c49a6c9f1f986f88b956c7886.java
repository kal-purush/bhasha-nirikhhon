package deneme;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

import java.time.Duration;

public class C01_deneme {
    //1. Bir Class olusturalim YanlisEmailTesti
    //2. http://automationpractice.com/index.php sayfasina gidelim
    //3. Sign in butonuna basalim
    //4. Email kutusuna @isareti olmayan bir mail yazip enter’a
    //bastigimizda “Invalid email address” uyarisi ciktigini test edelim

    WebDriver driver;

    @Before
    public void setup() {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(15));

    }


    @Test
    public void test01() {
        driver.get("http://automationpractice.com/index.php");

    }


    @After
    public void teardown() {
        //driver.close();
    }

}
