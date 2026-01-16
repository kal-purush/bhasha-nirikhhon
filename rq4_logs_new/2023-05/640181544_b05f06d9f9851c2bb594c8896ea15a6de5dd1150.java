package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class Nur_POM extends MyMethods {
    public Nur_POM() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "//span[text()='Education']")
    private WebElement educationBtn;
    @FindBy(xpath = "(//span[text()='Setup'])[5]")
    private WebElement setupUnderEducation;
    @FindBy(xpath = "(//span[contains(text(),'Scheme')])[1]")
    private WebElement gradingScheme;
    @FindBy(css= "svg[data-icon='plus']")
    private WebElement addBtn;
    @FindBy(css="input[data-placeholder='Name']")
    private WebElement inputNameBtn;
    @FindBy(xpath = "(//span[text()='Type'])[1]")
    private WebElement typeDropDown;
    @FindBy(xpath="(//span[contains(text(),'Point')])[2]")
    private WebElement pointUnderTypeBtn;
    @FindBy(xpath = "(//span[@class='mat-slide-toggle-bar'])[1]")
    private WebElement sliderEnableBtn;
    @FindBy(xpath = "//span[text()='Save']")
    private WebElement saveBtn;
    @FindBy(css = "svg[data-icon='pen-to-square']")
    private WebElement editBtn;
    @FindBy(css = "svg[data-icon='trash-can']")
    private WebElement deleteBtn;
    @FindBy(xpath = "//span[contains(text(),'Delete')]")
    private WebElement deleteConfirmationBtn;
    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessage;
    @FindBy(xpath = "//div[contains(text(),'already')]")
    private WebElement warningMessage;
    @FindBy(xpath = "//div//mat-error[contains(text(),'required')]")
    private WebElement thisFieldRequiredText;

    public WebElement getEducationBtn() {
        return educationBtn;
    }

    public WebElement getSetupUnderEducation() {
        return setupUnderEducation;
    }

    public WebElement getGradingScheme() {
        return gradingScheme;
    }

    public WebElement getAddBtn() {
        return addBtn;
    }

    public WebElement getInputNameBtn() {
        return inputNameBtn;
    }

    public WebElement getPointUnderTypeBtn() {
        return pointUnderTypeBtn;
    }

    public WebElement getSliderEnableBtn() {
        return sliderEnableBtn;
    }

    public WebElement getEditBtn() {
        return editBtn;
    }

    public WebElement getDeleteBtn() {
        return deleteBtn;
    }

    public WebElement getDeleteConfirmationBtn() {
        return deleteConfirmationBtn;
    }

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getWarningMessage() {
        return warningMessage;
    }

    public WebElement getTypeDropDown() {
        return typeDropDown;
    }

    public WebElement getSaveBtn() {
        return saveBtn;
    }

    public WebElement getThisFieldRequiredText() {
        return thisFieldRequiredText;
    }
}