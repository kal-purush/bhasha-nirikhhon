package pages.automationpractice;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import pages.BasePage;
import utils.PropertyReader;

public class MainPage extends BasePage {

    public String baseUrl = PropertyReader
            .getPropertyFromFile("properties/automationpractice.properties", "baseUrl");

    @FindBy(css = "#editorial_block_center h1")
    public WebElement h1OfexpectedText;

    public TopMenuBlock topMenuBlock;

    public MainPage() {
        super();
        topMenuBlock = new TopMenuBlock();
    }

    private String url = baseUrl + "/index.php";

    public boolean isInitialized() { return h1OfexpectedText.isDisplayed(); }

    public String getUrl() {
        return url;
    }

    public String expectedTextOnPage() {
        return "Automation Practice Website";
    }

}