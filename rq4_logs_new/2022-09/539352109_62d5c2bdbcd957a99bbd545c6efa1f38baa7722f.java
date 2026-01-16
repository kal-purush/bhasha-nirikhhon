import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.util.Random;

    public class register {
        @Test
        public void trans () throws InterruptedException {
            System.setProperty("webdriver.chrome.driver", "resources/chromedriver.exe");
            WebDriver driver = new ChromeDriver();

            driver.get("https://demo.midtrans.com/");

            WebElement buyBtn = driver.findElement(By.xpath("//a[@class='btn buy']"));
            WebElement user = driver.findElement(By.xpath("//input[@type=\"text\"]"));
            WebElement email = driver.findElement(By.xpath("//input[@type=\"email\"]"));
            WebElement phone = driver.findElement(By.xpath("//input[@value=\"081808466410\"]"));

            String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            StringBuilder sb = new StringBuilder();
            Random random = new Random();

            int length = 12;
            for (int i = 0; i < length; i++) {
                int value = random.nextInt(chars.length());
                char campur = chars.charAt(value);
                sb.append(campur);
            }
            String number = "1234567890";
            StringBuilder no = new StringBuilder();
            Random numbRb = new Random();

            driver.quit();

        }

    }
