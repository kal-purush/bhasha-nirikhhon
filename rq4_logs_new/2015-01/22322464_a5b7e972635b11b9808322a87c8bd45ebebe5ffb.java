package org.openqa.selenium.TBTest;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class TBTest  {
    public static void main(String[] args) {
        // Create a new instance of the html unit driver
        // Notice that the remainder of the code relies on the interface, 
        // not the implementation.
        WebDriver driver = new FirefoxDriver();

        // And now use this to visit Google
//        driver.get("http://www.google.com");
        driver.get("file:///home/keith/Development/js/index.html");
        WebElement tab1 = driver.findElement(By.id("tab1"));
        WebElement tab2 = driver.findElement(By.id("tab2"));
        WebElement tab3 = driver.findElement(By.id("tab3"));
        outTest(driver);
        tab3.click();
        outTest(driver);
        tab2.click();
        outTest(driver);
        tab1.click();
        outTest(driver);

/*        JavascriptExecutor js = (JavascriptExecutor) driver;
        js.executeScript("document.getElementById('natsp').style.display='block';");
        linka.submit();
//        driver.findElement(By.partialLinkText("GLBThotline")).click();
        // Find the text input element by its name
        WebElement element = driver.findElement(By.name("q"));

        // Enter something to search for
        element.sendKeys("Cheese!");

        // Now submit the form. WebDriver will find the form for us from the element
        element.submit();
*/
        // Check the title of the page
        System.out.println("Page title is: " + driver.getTitle());

        driver.quit();
    }
    public static void outTest(WebDriver driver) {
        try { 
        	WebElement about  = driver.findElement(By.id("abouth2"));
            System.out.println("About heading: " + about.getText());
        }
        catch (Exception e) {
        	System.out.println("No abouth2 id found");
        }
        try { 
            WebElement advant = driver.findElement(By.id("advanth2"));
            System.out.println("Advant heading: " + advant.getText());
        }
        catch (Exception e) {
        	System.out.println("No advanth2 id found");
        }
        try { 
        	WebElement using  = driver.findElement(By.id("using2"));
        	System.out.println("Using heading: " + using.getText());
        }
        catch (Exception e) {
        	System.out.println("No usingh2 id found");
        }
    }
}