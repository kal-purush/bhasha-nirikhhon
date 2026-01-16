package Pages;

import Utilities.GWD;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.Select;

public class DialogContent_Ulvi extends ParentPage {
    public DialogContent_Ulvi() {
        PageFactory.initElements(GWD.getDriver(), this);
    }
    @FindBy(css = "[name='login[username]']")
    public WebElement emailInput;

    @FindBy(css = "[name='login[password]']")
    public WebElement passwordInput;

    @FindBy(css = "[name='firstname']")
    public WebElement firstNameInput;

    @FindBy(css = "[name='lastname']")
    public WebElement lastNameInput;

    @FindBy(css = "[name='street[]']")
    public WebElement streetInput;

    @FindBy(css = "[name='city']")
    public WebElement cityInput;

    @FindBy(css = "[name='region_id']")
    public WebElement stateInput;

    @FindBy(css = "[name='postcode']")
    public WebElement postCodeInput;

    @FindBy(css = "[name='country_id']")
    public WebElement countryIdInput;

    @FindBy(css = "[name='telephone']")
    public WebElement phoneInput;

    public WebElement getWebElement(String strElement) {
        switch (strElement) {
            case "firstNameInput":
                return this.firstNameInput;
            case "lastNameInput":
                return this.lastNameInput;
            case "streetInput":
                return this.streetInput;
            case "cityInput":
                return this.cityInput;
            case "stateInput":
                return this.stateInput;
            case "postCodeInput":
                return this.postCodeInput;
            case "countryIdInput":
                return this.countryIdInput;
            case "phoneInput":
                return this.phoneInput;
        }
        return null;
    }}