package pageObjects;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.WebDriverWait;

public class BasePage {
	
	WebDriver driver;
	public BasePage(WebDriver driver) {
		this.driver=driver;
		PageFactory.initElements(driver,this);
	}
	
	public void Scrollby(WebElement tothisElement)
	{
		Actions action = new Actions(driver);
		action.moveToElement(tothisElement).build().perform();
	}
	
	public void screenShot(String ss) throws IOException {
		TakesScreenshot ts = (TakesScreenshot) driver;

		File src = ts.getScreenshotAs(OutputType.FILE);
		File trg = new File("C:\\Users\\2303448\\eclipse-workspace\\casproject\\screenshots\\" + ss + ".png");
		FileUtils.copyFile(src, trg);
	}

	public void scroll(WebElement toThisElement) {
		Actions ac = new Actions(driver);
		ac.scrollToElement(toThisElement);
	}

	public void scrollToBottom() {
		JavascriptExecutor js = (JavascriptExecutor) driver;
		js.executeScript("window.scrollBy(0,document.body.scrollHeight)");
	}

	public void explicitWait(WebElement elementLocated) {
		WebDriverWait myWait = new WebDriverWait(driver, Duration.ofSeconds(10));
		myWait.until(ExpectedConditions.visibilityOf(elementLocated));

	}

	public void fluentWait(WebElement fwElement) {
		FluentWait<WebDriver> fw = new FluentWait<WebDriver>(driver);
		fw.withTimeout(Duration.ofSeconds(10));
		fw.pollingEvery(Duration.ofSeconds(1));
		fw.ignoring(NoSuchElementException.class);

		fw.until(ExpectedConditions.visibilityOf(fwElement));
	}

}