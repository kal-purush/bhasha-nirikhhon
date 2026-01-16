
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;


public class arvutus {
    private static WebDriver driver = null;
    @Test
    public void checkAnswers() throws Exception {
        System.setProperty("webdriver.chrome.driver", "D:\\Java\\JARs\\chromedriver.exe");
        driver = new ChromeDriver();
        driver.get("http://www.tlu.ee/~shev/javakt2/");
        driver.findElement(By.id("n")).sendKeys("5");
        driver.findElement(By.id("m")).sendKeys("3");
        driver.findElement(By.id("btn")).click();
        WebElement strvalue = driver.findElement(By.xpath("//*[@id=\"tulemus\"]"));
        String expected = "10";
        String actual = strvalue.getText();
        System.out.println(actual);
        if(expected.equals(actual)) {
            System.out.println("Pass");
        }else{
            System.out.println("Fail");
        }
        driver.quit();          
    }
    @Test
    public void calculataAnswers() throws Exception {
        
        System.setProperty("webdriver.chrome.driver", "D:\\Java\\JARs\\chromedriver.exe");
        driver = new ChromeDriver();
        driver.get("http://www.tlu.ee/~shev/javakt2/");
        driver.findElement(By.id("n")).sendKeys("5");
        driver.findElement(By.id("m")).sendKeys("3");
        driver.findElement(By.id("btn")).click();
        WebElement strvalue = driver.findElement(By.xpath("//*[@id=\"tulemus\"]"));
        int tulemus = ((5*4*3*2*1)/((3*2*1)*(2*1)));
        String expected = Integer.toString(tulemus);
        String actual = strvalue.getText();
        System.out.println(actual);
        if(expected.equals(actual)) {
            System.out.println("Pass");
        }else{
            System.out.println("Fail");
        }
        driver.quit();          
    }
}   
   