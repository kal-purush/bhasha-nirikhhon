package Pages;

import Utilities.GWD;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class DialogContent_Zehra extends ParentPage{
    public DialogContent_Zehra() {
        PageFactory.initElements(GWD.getDriver(), this);
    }
    @FindBy(xpath = "//*[text()='Women']")
    public WebElement womenTab;
    @FindBy(xpath = "(//*[text()='Bottoms'])[1]")
    public WebElement bottomsTab;
    @FindBy(xpath = "(//*[text()='Shorts'])[1]")
    public WebElement shortsTab;
    @FindBy(xpath = "//div[@class='products wrapper grid products-grid']/ol/li")
    public WebElement shortsSelection;



}