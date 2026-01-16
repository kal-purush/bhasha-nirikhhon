package locating;

import myframe.Constants;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

public class FindObjects {

	public WebElement locateElement (String logicalName){
		String locatorString = Constants.OR.getProperty(logicalName);
		int indexOfEqulTo = locatorString.indexOf("=");
		String objectPropertyName = locatorString.substring(0, indexOfEqulTo);
		String objectPropertyValue = locatorString.substring(indexOfEqulTo+1,locatorString.length());
		WebElement foundElement = null;
		
		System.out.println("objectPropertyName = "+objectPropertyName);
		System.out.println("objectPropertyValue = "+objectPropertyValue);
		if(objectPropertyName.equalsIgnoreCase("id")){
			foundElement = Constants.browser.findElement(By.id(objectPropertyValue));
		}
		
		if(objectPropertyName.equalsIgnoreCase("name")){
			foundElement = Constants.browser.findElement(By.name(objectPropertyValue));
		}
		if(objectPropertyName.equalsIgnoreCase("xpath")){
			foundElement = Constants.browser.findElement(By.xpath(objectPropertyValue));
		}
		if(objectPropertyName.equalsIgnoreCase("cssSelector")){
			foundElement = Constants.browser.findElement(By.cssSelector(objectPropertyValue));
		}
		if(objectPropertyName.equalsIgnoreCase("linkText")){
			foundElement = Constants.browser.findElement(By.linkText(objectPropertyValue));
		}
		if(objectPropertyName.equalsIgnoreCase("partialLinkText")){
			foundElement = Constants.browser.findElement(By.partialLinkText(objectPropertyValue));
		}
		return foundElement;
	}
}