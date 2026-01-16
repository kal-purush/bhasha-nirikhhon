package com.clarecontrols.clarefusion.extensions;

import java.util.List;
import org.openqa.selenium.JavascriptExecutor; 
import org.openqa.selenium.WebDriver; 
import org.openqa.selenium.WebDriverException; 
import org.openqa.selenium.WebElement;

@SuppressWarnings("unchecked")
public class SizzleSelector {
	private JavascriptExecutor driver;

	public SizzleSelector(WebDriver webDriver) { 
		driver = (JavascriptExecutor) webDriver; 
	}

	public WebElement findElementBySizzleCss(String using) { 
		injectSizzleIfNeeded(); 
		String javascriptExpression = createSizzleSelectorExpression(using); 
		List<WebElement> elements = (List<WebElement>) 
		driver .executeScript(javascriptExpression); 
		if (elements.size() > 0) 
			return (WebElement) 
		elements.get(0); 
		return null; 
	} 

	public List<WebElement> findElementsBySizzleCss(String using) { 
		injectSizzleIfNeeded(); 
		String javascriptExpression = createSizzleSelectorExpression(using); 
		return (List<WebElement>) driver.executeScript(javascriptExpression); 
	} 

	private String createSizzleSelectorExpression(String using) { 
		return "return Sizzle(\"" + using + "\")"; 
	} 
			
	private void injectSizzleIfNeeded() { 
		if (!sizzleLoaded()) injectSizzle(); 
	} public Boolean sizzleLoaded() { 
		Boolean loaded; 
		try { 
			loaded = (Boolean) driver.executeScript("return Sizzle()!=null"); 
		} catch (WebDriverException e) { 
			loaded = false; 
		} return loaded; 
	} 
	public void injectSizzle() { 
		driver.executeScript(" var headID = document.getElementsByTagName(\"head\")[0];" + 
			"var newScript = document.createElement('script');" + 
			"newScript.type = 'text/javascript';" + 
			"newScript.src = 'http://cdnjs.cloudflare.com/ajax/libs/sizzle/1.10.5/sizzle.min.js';"
			+ "headID.appendChild(newScript);"); 
	} 
}