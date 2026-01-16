package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class Utkucan_POM extends MyMethods {
    public Utkucan_POM() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "//span[text()='Human Resources']")
    private WebElement humanResources;

    @FindBy(xpath = "//span[text()='Salary']")
    private WebElement salary;
    @FindBy(xpath = "//span[text()='Position Salary']")
    private WebElement positionSalaryUnderHumanResources;
    @FindBy(css= "svg[data-icon='plus']")
    private WebElement addButtonForPositionSalary;
    @FindBy(xpath = "//input[@type='text']")
    private WebElement inputPositionSalaryNameButton;

    @FindBy(xpath = "//mat-dialog-actions[contains(@class, 'mat-dialog-actions-align-end')]")
    private WebElement emptyPart;
    @FindBy(xpath = "//mat-error[text()=' This field is required!']")
    private WebElement nameWarning;

    @FindBy(xpath = "//input[@id='ms-text-field-9']")
    private WebElement inputPositionSalaryNameButtonAfterEdit;
    @FindBy(css = "svg[data-icon='trash-can']")
    private WebElement deleteButtonForPositionSalary;

    @FindBy(xpath ="//span[normalize-space()='Delete']")
    private WebElement sureDeleteButtonForPositionSalary;

    @FindBy(css = "svg[data-icon='pen-to-square']")
    private WebElement editButtonForPositionSalary;

    @FindBy(xpath ="//span[contains(text(),'Save')]")
    private WebElement saveButtonForPositionSalary;
    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessageForPositionSalary;

    public WebElement getHumanResources() {
        return humanResources;
    }

    public WebElement getSalary(){ return salary; }

    public WebElement getPositionSalaryUnderHumanResources() {
        return positionSalaryUnderHumanResources;
    }

    public WebElement getAddButtonForPositionSalary() {
        return addButtonForPositionSalary;
    }

    public WebElement getInputPositionSalaryNameButton() {
        return inputPositionSalaryNameButton;
    }

    public WebElement getEmptyPart() { return emptyPart; }

    public WebElement getNameWarning() { return nameWarning; }

    public WebElement getInputPositionSalaryNameButtonAfterEdit() { return inputPositionSalaryNameButtonAfterEdit; }

    public WebElement getDeleteButtonForPositionSalary(){
        return deleteButtonForPositionSalary;
    }

    public WebElement getSureDeleteButtonForPositionSalary(){
        return sureDeleteButtonForPositionSalary;
    }

    public WebElement getEditButtonForPositionSalary(){
        return editButtonForPositionSalary;
    }

    public WebElement getSaveButtonForPositionSalary() {
        return saveButtonForPositionSalary;
    }

    public WebElement getSuccessMessageForPositionSalary(){
        return successMessageForPositionSalary;
    }
}