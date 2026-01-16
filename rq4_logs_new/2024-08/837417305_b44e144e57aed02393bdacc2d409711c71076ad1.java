package com.fitpeoAssignment;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;

public class FitpeoAssignment {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("WebDriver.chrome.driver","C:\\Users\\asisc\\Downloads\\Selenium JAR & sw\\chromedriver.exe");
		ChromeDriver cd = new ChromeDriver();
		cd.manage().window().maximize();

		/*1.Navigate to the FitPeo Home page*/
		cd.get("https://www.fitpeo.com/");

		/*2.From the home page, navigate to the Revenue Calculator Page*/
		cd.findElement(By.xpath("//div[text()='Revenue Calculator']")).click();
		cd.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);

		/*3.Scroll Down to the Slider section*/
		((JavascriptExecutor) cd).executeScript("window.scrollBy(0, 450)");

		/*4.Adjust the Slider*/
		WebElement revenueCalculatorSlider = cd.findElement(By.xpath("//span[@class='MuiSlider-thumb MuiSlider-thumbSizeMedium MuiSlider-thumbColorPrimary MuiSlider-thumb MuiSlider-thumbSizeMedium MuiSlider-thumbColorPrimary css-sy3s50']"));
		WebElement textField = cd.findElement(By.xpath("//body/div/div/div/div[2]/div/div/div/div/input"));
		Actions a = new Actions(cd);
		a.clickAndHold(revenueCalculatorSlider).build().perform();
		String textFieldValue = textField.getAttribute("value");
		int targetValue = 820;
		while(Integer.parseInt(textFieldValue) != targetValue-1)
		{
			textFieldValue = textField.getAttribute("value");
			a.sendKeys(Keys.ARROW_RIGHT).build().perform();
		}
		if(Integer.parseInt(textFieldValue)==targetValue-1) {
			System.out.println("Bottom text field value updated to 820");
		}

		/*5.Update the Text Field*/
		textField.click();
		int valueLenght = textField.getAttribute("value").length(); 
		for(int i = 1 ; i <= valueLenght ; i++) {
			a.sendKeys(Keys.BACK_SPACE).build().perform();
		}
		textField.sendKeys("560");
		/*6.Validate Slider Value*/
		WebElement RevenueCalculatorSliderValue = cd.findElement(By.xpath("//body/div/div/div/div[2]/div/div/span/span[3]/input"));
		if(Integer.parseInt(RevenueCalculatorSliderValue.getAttribute("value"))==560) {
			System.out.println("Slider's position is updated to reflect the value 560");
		}
		Thread.sleep(2500);
		
		/*7.Select CPT Codes*/
		WebElement cptCheckBox99091 = cd.findElement(By.xpath("(//input[@type='checkbox'])[3]"));
		a.scrollToElement(cptCheckBox99091).build().perform();
		cd.findElement(By.xpath("(//input[@type='checkbox'])[1]")).click();
		cd.findElement(By.xpath("(//input[@type='checkbox'])[2]")).click();
		cptCheckBox99091.click();
		WebElement cptCheckBox99474 = cd.findElement(By.xpath("(//input[@type='checkbox'])[8]"));
		a.scrollToElement(cptCheckBox99474).build().perform();
		cptCheckBox99474.click();
		Thread.sleep(2500);
		
		/*8.Validate Total Recurring Reimbursement*/
		a.scrollToElement(textField).build().perform();
		textField.click();
		for(int i = 1 ; i <= valueLenght ; i++) {
			a.sendKeys(Keys.BACK_SPACE).build().perform();
		}
		textField.sendKeys("820");
		
		/*9.Verify that the header displaying Total Recurring Reimbursement for all Patients Per Month: shows the value $110700.*/
		a.scrollToElement(cptCheckBox99474).build().perform();
		WebElement total = cd.findElement(By.xpath("//body/div/div/header/div/p[4]/p"));
		String totalReccuring = total.getText();
		String totalRecurringReimbursement = "" ;
		for(int i = 1 ; i <= totalReccuring.length()-1 ; i++) {
			totalRecurringReimbursement = totalRecurringReimbursement + totalReccuring.charAt(i);
		}
		if(Integer.parseInt(totalRecurringReimbursement)==110700) {
			System.out.println("Total Recurring Reimbursement for all Patients Per Month:$"+totalRecurringReimbursement);
		}
		
	}
}