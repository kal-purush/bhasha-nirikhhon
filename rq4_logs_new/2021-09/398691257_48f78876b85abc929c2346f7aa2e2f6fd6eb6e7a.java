package test.java.DemoQa.controlExtension;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class TextboxControlExtension {
	
	private WebElement mappedElement;
	private WebDriver driver;
	
	public TextboxControlExtension(WebElement mappedElement, WebDriver driver){
		this.mappedElement=mappedElement;		
		this.driver=driver;
	}
	
	public void setValue(String value) {
	
		mappedElement.sendKeys(value);
		waitValueEnter(value);
	}
	
	public String getValue() {
		waitValue();
		String[] test = mappedElement.getText().split(":");
		return test[1];
	}
	public void waitValue(){
		WebDriverWait wait = new WebDriverWait(this.driver,5);
		wait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.xpath("//div[@class='border col-md-12 col-sm-12']")));
		
	}
	
	public void waitValueEnter(String value) {
		WebDriverWait wait = new WebDriverWait(this.driver,5);
		wait.until(ExpectedConditions.attributeToBe(mappedElement, "value", value));
		
	}
}