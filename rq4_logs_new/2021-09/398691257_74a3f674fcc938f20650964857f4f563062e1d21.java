import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class ClassicSeriesPage extends PageObject {
	protected ClassicSeriesPage(ChromeDriver driver, String baseUrl) {
		super(driver, baseUrl);
	}
	
	public SVT410HLFDetailPage clickSVT410HLFDetailPage() {
		
		WebElement menuElement = getSvtProProductPageElement("SVT-410HLF");
		
		if (WebWaitTimeInitemProductPage("Ampeg: Classic Series")==true)
			menuElement.click();

		return new SVT410HLFDetailPage(this.driver, this.baseUrl);
	}		
}