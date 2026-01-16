package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.util.List;

public class DialogContent extends MyMethods {
    public DialogContent() {
        PageFactory.initElements(BasicDriver.getDriver(),this);
    }
    @FindBy(css = "input[placeholder='Username']")
    private WebElement loginUsername;

    @FindBy(css = "input[placeholder='Password']")
    private WebElement loginPassword;

    @FindBy(xpath = "//span[contains(text(),'LOGIN')]")
    private WebElement loginBtn;

    @FindBy(xpath = "(//span[text()='Setup'])[1]")
    private WebElement setupBtn;

    @FindBy(xpath = "//span[text()='Parameters']")
    private WebElement parameterBtn;

    @FindBy(xpath = "(//span[text()='Countries'])[1]")
    private WebElement countriesBtn;

    @FindBy(css = "svg[data-icon='plus']")
    private WebElement addCountry;

    @FindBy(xpath = "(//input[@data-placeholder='Name'])[2]")
    private WebElement countryName;

    @FindBy(xpath = "(//input[@data-placeholder='Name'])[1]")
    private WebElement countryNameSearch;

    @FindBy(xpath = "(//input[@data-placeholder='Code'])[2]")
    private WebElement countryCode;

    @FindBy(xpath = "(//input[@data-placeholder='Code'])[1]")
    private WebElement countryCodeSearch;

    @FindBy(xpath = "//span[text()='Citizenships']")
    private WebElement citizenshipBtn;

    @FindBy(xpath = "(//input[@data-placeholder='Name'])[2]")
    private WebElement citizenshipName;

    @FindBy(xpath = "(//input[@data-placeholder='Short Name'])[2]")
    private WebElement citizenshipShortName;

    @FindBy(xpath = "(//input[@data-placeholder='Name'])[1]")
    private WebElement citizenshipNameSearch;

    @FindBy(xpath = "//span[text()='States']")
    private WebElement statesBtn;

    @FindBy(xpath = "//tbody[@role='rowgroup']//td[2]")
    private List<WebElement> statesList;

    public List<WebElement> getStatesList() {
        return statesList;
    }

    public WebElement getStatesBtn() {
        return statesBtn;
    }

    public WebElement getCitizenshipNameSearch() {
        return citizenshipNameSearch;
    }

    public WebElement getCitizenshipShortNameSearch() {
        return citizenshipShortNameSearch;
    }

    @FindBy(xpath = "(//input[@data-placeholder='Short Name'])[1]")
    private WebElement citizenshipShortNameSearch;

    public WebElement getCitizenshipBtn() {
        return citizenshipBtn;
    }

    public WebElement getAddCountry() {
        return addCountry;
    }

    public WebElement getCountryName() {
        return countryName;
    }

    public WebElement getCountryCode() {
        return countryCode;
    }

    public WebElement getCountrySaveBtn() {
        return countrySaveBtn;
    }

    @FindBy(xpath = "//span[text()='Save']")
    private WebElement countrySaveBtn;

    public WebElement getCitizenshipName() {
        return citizenshipName;
    }

    public WebElement getCitizenshipShortName() {
        return citizenshipShortName;
    }

    public WebElement getCitizenshipSaveBtn() {
        return citizenshipSaveBtn;
    }

    @FindBy(xpath = "//span[text()='Save']")
    private WebElement citizenshipSaveBtn;

    @FindBy(css = "ms-delete-button")
    private WebElement deleteCountry;

    @FindBy(xpath = "//span[contains(text(),'Delete')]")
    private WebElement confirmDeleteCountry;

    public WebElement getConfirmDeleteCountry() {
        return confirmDeleteCountry;
    }

    public WebElement getDeleteCountry() {
        return deleteCountry;
    }

    public WebElement getCountryNameSearch() {
        return countryNameSearch;
    }

    public WebElement getCountryCodeSearch() {
        return countryCodeSearch;
    }

    public WebElement getCountrySearchBtn() {
        return countrySearchBtn;
    }

    @FindBy(xpath = "//span[text()='Search']")
    private WebElement countrySearchBtn;

    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessage;

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getSetupBtn() {
        return setupBtn;
    }

    public WebElement getParameterBtn() {
        return parameterBtn;
    }

    public WebElement getCountriesBtn() {
        return countriesBtn;
    }

    @FindBy(xpath = "//div[@class='ng-star-inserted']//span[contains(text(),'Dashboard')]")
    private WebElement dashBoardHeader;

    public WebElement getDashBoardHeader() {
        return dashBoardHeader;
    }

    public WebElement getLoginUsername() {
        return loginUsername;
    }

    public WebElement getLoginPassword() {
        return loginPassword;
    }

    public WebElement getLoginBtn() {
        return loginBtn;
    }
}