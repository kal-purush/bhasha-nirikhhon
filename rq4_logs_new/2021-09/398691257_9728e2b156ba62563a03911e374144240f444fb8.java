package ampeg.pageObjects;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import framework.PageObject;

public class AmpegClassicSeriesPage extends PageObject {
	protected AmpegClassicSeriesPage(WebDriver driver, String baseUrl) {
		super(driver, baseUrl);
	}
	
	public SVT410HLFDetailPage clickSVT410HLFDetailPage() {
		
		WebElement menuElement = getClassicProductPageElement("SVT-410HLF");
		
		menuElement.click();

		return new SVT410HLFDetailPage(this.driver, this.baseUrl);
	}	
	
	public WebElement getClassicProductPageElement(String classicProductElement) {
		return driver.findElement(By.xpath("//tbody//a[contains(text(), '" + classicProductElement + "')]"));
	}
}