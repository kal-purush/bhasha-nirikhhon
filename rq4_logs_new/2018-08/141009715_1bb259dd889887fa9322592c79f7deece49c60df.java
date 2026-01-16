package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class MainPage {
    private WebDriver driver;

    public MainPage(WebDriver driver)
    {
        this.driver = driver;
    }
    String inputcss = "#twotabsearchtextbox";
    public void open(){driver.get("https://www.amazon.com/");}
    public SearchResultsPage searchForText(String text){
        WebElement inputField = driver.findElement(By.cssSelector(inputcss));
        inputField.sendKeys(text);
        inputField.sendKeys(Keys.ENTER);
        return new SearchResultsPage(driver);
    }


}