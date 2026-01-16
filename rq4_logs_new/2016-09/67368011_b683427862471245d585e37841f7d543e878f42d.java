package addJeansToShoppingCart;

import java.util.List;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.Select;

public class AddJeans {

	public static void main(String[] args) {
		System.setProperty("webdriver.chrome.driver", "C:\\LearnSelenium\\chromedriver.exe");
		WebDriver	driver = new ChromeDriver();
        String appUrl = "https://preview.debijenkorf.nl";        

       //launch the Chrome browser and open the application url
        driver.get(appUrl);       

       //maximize the browser window
        driver.manage().window().maximize();        

        //Clearing the data from the search box and entering jeans
        driver.findElement(By.cssSelector("input.dbk-form--field")).clear();
        driver.findElement(By.cssSelector("input.dbk-form--field")).sendKeys("jeans");        
        driver.findElement(By.cssSelector("button.dbk-form--submit")).click();
                
        //select 2nd search result
        driver.findElement(By.cssSelector("ul > li:nth-child(2) > div > a")).click();
        
        // select size from the dropdown
        Select jeanssize = new Select(driver.findElement(By.cssSelector("select.dbk-form--input")));
        List<WebElement> alloptions = jeanssize.getOptions();
        
        //Finding the add to shopping cart button
        WebElement button = driver.findElement(By.xpath("//div[@class='dbk-btn-group--dual dbk-btn-group--dual_visible-xs-condensed']/button"));
        
        for(int i=1 ;i<=alloptions.size();i++)
        {
            jeanssize.selectByIndex(i);
            if (button.getText().equalsIgnoreCase("in winkelmand"))
            {
            	button.click();
            	break;
            }
        }
        
        //close the web browser
        driver.close();        
	}

}