package PageObjects;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindAll;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class WidgetsPage {

    private static WebDriver driver;

    public WidgetsPage(WebDriver driver) {
        this.driver = driver;
        PageFactory.initElements(driver,this);
    }

    @FindBy(xpath = "//body/div[@id='app']/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]/div[4]/div[1]/ul[1]/li[3]")
    public WebElement BTNWebDatePicker;

    @FindBy(xpath = "//*[@id=\"datePickerMonthYearInput\"]")
    public WebElement INPDate;

    @FindBy(xpath = "//*[@id=\"dateAndTimePickerInput\"]")
    public WebElement SLCDateTime;

    @FindBy(xpath = "//*[@id=\"dateAndTimePicker\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[1]")
    public WebElement SLCMonthTime;

    @FindBy(xpath = "//*[@id=\"dateAndTimePicker\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/div")
    public WebElement SLCYearDateTime;

    @FindBy(xpath = "//*[@id=\"datePickerMonthYear\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[1]/select")
    public WebElement SLCMonth;

    @FindBy(xpath = "//*[@id=\"datePickerMonthYear\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/select")
    public WebElement SLCYear;

    private WebElement FindMonth(){
        int random = (int)(Math.random()*1+11);
        return driver.findElement(By.xpath("//*[@id=\"datePickerMonthYear\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[1]/select/option["+random+"]"));
    }

    private WebElement FindYear(){
        int random = (int)(Math.random()*201);
        random = random > 0 ? random : 1;
        return driver.findElement(By.xpath("//*[@id=\"datePickerMonthYear\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/select/option["+random+"]"));
    }

    private  WebElement FindDay(){
        int random = (int)(Math.random()*1+28);
        return driver.findElement(By.xpath("//div[contains(text(),'"+random+"')]"));
    }

    public void SelectDate(){
        JavascriptExecutor ex = (JavascriptExecutor) driver;
        ex.executeScript("arguments[0].click();", FindMonth());
        ex.executeScript("arguments[0].click();", FindYear());
        ex.executeScript("arguments[0].click();", FindDay());
    }

    private WebElement FindMonthDateTime(){
        int random = (int)(Math.random()*1+11);
        return driver.findElement(By.xpath("//body/div[@id='app']/div[1]/div[1]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[1]/div[1]/div[2]/div[1]/div[2]/div[1]/div[1]/div["+random+"]"));
    }

    private WebElement FindYearDateTime(){
        int random = (int)(Math.random()*13+1);
        return driver.findElement(By.xpath("//*[@id=\"dateAndTimePicker\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/div[1]/div["+random+"]"));
    }

    private WebElement FindDayDateTime(){
        int random = (int)(Math.random()*4+1);
        return driver.findElement(By.xpath("//*[@id=\"dateAndTimePicker\"]/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div["+random+"]"));
    }

    private WebElement FindTime(){
        int random = (int)(Math.random()*95+1);
        return driver.findElement(By.xpath("//*[@id=\"dateAndTimePicker\"]/div[2]/div[2]/div/div/div[3]/div[2]/div/ul/li["+random+"]"));
    }

    public void SelectDateTime(){
        JavascriptExecutor ex = (JavascriptExecutor) driver;
        ex.executeScript("arguments[0].click();", FindMonthDateTime());
        ex.executeScript("arguments[0].click();", FindYearDateTime());
        ex.executeScript("arguments[0].click();", FindDayDateTime());
        ex.executeScript("arguments[0].click();", FindTime());
    }
}