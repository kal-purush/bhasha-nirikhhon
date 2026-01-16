package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class Alex_POM extends MyMethods {
    //from here Step Definisn take info - u not able to work in step def before not finished up with POM file!!

    public Alex_POM() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "//span[text()='Education']")
    private WebElement educationBtn;
    @FindBy(xpath = "(//span[text()='Setup'])[5]")
    private WebElement setupUnderEducation;

    //    //span[contains(text(),'Subject Categories')])[1]
    @FindBy(xpath = "(//span[contains(text(),'Subject')])[1]")
    private WebElement subjectCategories;

    @FindBy(css = "svg[data-icon='plus']")
    private WebElement addBtn;

    @FindBy(css = "(input[data-placeholder='Name'])[2]")
    private WebElement inputNameBtn;

    @FindBy(xpath = "(//span[text()='Code'])[2]")
    private WebElement typeCode;

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

    public void setEducationBtn(WebElement educationBtn) {
        this.educationBtn = educationBtn;
    }

    public WebElement getSetupUnderEducation() {
        return setupUnderEducation;
    }

    public void setSetupUnderEducation(WebElement setupUnderEducation) {
        this.setupUnderEducation = setupUnderEducation;
    }

    public WebElement getSubjectCategories() {
        return subjectCategories;
    }

    public void setSubjectCategories(WebElement subjectCategories) {
        this.subjectCategories = subjectCategories;
    }

    public WebElement getAddBtn() {
        return addBtn;
    }

    public void setAddBtn(WebElement addBtn) {
        this.addBtn = addBtn;
    }

    public WebElement getInputNameBtn() {
        return inputNameBtn;
    }

    public void setInputNameBtn(WebElement inputNameBtn) {
        this.inputNameBtn = inputNameBtn;
    }

    public WebElement getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(WebElement typeCode) {
        this.typeCode = typeCode;
    }

    public WebElement getSliderEnableBtn() {
        return sliderEnableBtn;
    }

    public void setSliderEnableBtn(WebElement sliderEnableBtn) {
        this.sliderEnableBtn = sliderEnableBtn;
    }

    public WebElement getSaveBtn() {
        return saveBtn;
    }

    public void setSaveBtn(WebElement saveBtn) {
        this.saveBtn = saveBtn;
    }

    public WebElement getEditBtn() {
        return editBtn;
    }

    public void setEditBtn(WebElement editBtn) {
        this.editBtn = editBtn;
    }

    public WebElement getDeleteBtn() {
        return deleteBtn;
    }

    public void setDeleteBtn(WebElement deleteBtn) {
        this.deleteBtn = deleteBtn;
    }

    public WebElement getDeleteConfirmationBtn() {
        return deleteConfirmationBtn;
    }

    public void setDeleteConfirmationBtn(WebElement deleteConfirmationBtn) {
        this.deleteConfirmationBtn = deleteConfirmationBtn;
    }

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public void setSuccessMessage(WebElement successMessage) {
        this.successMessage = successMessage;
    }

    public WebElement getWarningMessage() {
        return warningMessage;
    }

    public void setWarningMessage(WebElement warningMessage) {
        this.warningMessage = warningMessage;
    }

    public WebElement getThisFieldRequiredText() {
        return thisFieldRequiredText;
    }

    public void setThisFieldRequiredText(WebElement thisFieldRequiredText) {
        this.thisFieldRequiredText = thisFieldRequiredText;
    }
}