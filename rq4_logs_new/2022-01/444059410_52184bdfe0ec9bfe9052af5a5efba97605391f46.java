package org.test.run;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.KeyEvent;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

public class TestProject1 {
	public static void main(String[] args) throws AWTException, Exception {

		WebDriverManager.chromedriver().setup();
		WebDriver driver = new ChromeDriver();

		driver.get("https://www.flipkart.com/");
		driver.manage().window().maximize();
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);

		WebElement cancelbtn = driver.findElement(By.xpath("/html/body/div[2]/div/div/button"));
		cancelbtn.click();

		WebElement srchbox = driver.findElement(By.xpath("//input[@type='text']"));
		srchbox.sendKeys("iphone 13");

		Robot r = new Robot();
		r.keyPress(KeyEvent.VK_ENTER);
		r.keyRelease(KeyEvent.VK_ENTER);

		System.out.println("");
		System.out.println("----description------");
		List<WebElement> name = driver.findElements(By.xpath("//a[@class='_1fQZEK']"));
		for (int i = 0; i < name.size(); i++) {
			WebElement b = name.get(i);
			String bb = b.getText();
			System.out.println(bb);

		}

		System.out.println("");
		System.out.println("-----Mobiles Name------");
		List<WebElement> name1 = driver.findElements(By.xpath("//div[@class='_4rR01T']"));

		for (int i = 0; i < name1.size(); i++) {
			WebElement nn = name1.get(i);
			String txt1 = nn.getText();
			System.out.println(txt1);

		}
		
		System.out.println("");
		System.out.println("----price-----");
		List<WebElement> name2 = driver.findElements(By.xpath("//div[@class='_30jeq3 _1_WHN1']"));
		for (int j = 0; j < name2.size(); j++) {
			WebElement gg = name2.get(j);
			String g = gg.getText();
			System.out.println(g);
		}

		System.out.println("");
		System.out.println("----Mobile name and price list------");
		for (int i = 0; i < name1.size();) {
			for (int j = 0; j < name2.size(); j++) {
				WebElement ddf = name1.get(i);
				String ffd = ddf.getText();
				System.out.println(ffd);
				WebElement jjh = name2.get(j);
				String ppp = jjh.getText();
				System.out.println(ppp);
				i++;

			}

		}

		System.out.println("");
		System.out.println("-----price list-----");
		List<WebElement> name3 = driver.findElements(By.xpath("//div[text()='₹79,900']"));
		for (int i = 0; i < name3.size(); i++) {
			WebElement iii = name3.get(i);
			String iio = iii.getText();
			System.out.println(iio);

		}
		
		
		WebElement nextbtn = driver.findElement(By.xpath("//span[text()='Next']"));
		nextbtn.click();
		Thread.sleep(3000);
		
		
		System.out.println("------page 2-------");
		List<WebElement> listpage2 = driver.findElements(By.xpath("//div[@class='_4rR01T']"));
		for (int i = 0; i < listpage2.size(); i++) {
		WebElement kl = listpage2.get(i);
		String tr = kl.getText();
		System.out.println(tr);
		}
		
		
		
	}

}