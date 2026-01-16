package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class Salary_Type extends MyMethods {

    public Salary_Type() {
        PageFactory.initElements(BasicDriver.getDriver(),this);
    }
    @FindBy(xpath = "//span[text()='Human Resources']")
    private WebElement humRes;

    @FindBy(xpath = "//span[text()='Salary']")
    private WebElement salary;

    @FindBy(xpath = "//span[text()='Salary Types']")
    private WebElement salaryType;

    @FindBy(css = "svg[class='svg-inline--fa fa-plus']")
    private WebElement plusButton;

    @FindBy(css = ".svg-inline--fa.fa-pen-to-square")
    private WebElement editButton;

    @FindBy(css = "ms-delete-button")
    private WebElement deleteButton;

    @FindBy(xpath = "//input[@type='text']")
    private WebElement inputName;

    @FindBy(xpath = "//input[@placeholder='User Type']")
    private WebElement userType;

    @FindBy(xpath = "//div[@role='listbox']")
    private WebElement listBox;

    @FindBy(xpath = "//span[text()='Save']")
    private WebElement saveButton;

    @FindBy(xpath = "//div[text()='Salary Type successfully created']")
    private WebElement successMessage;

    @FindBy(xpath = "//div[text()='Salary Type successfully updated']")
    private WebElement editedSuccessMessage;

    @FindBy(xpath = "//div[text()='Salary Type successfully deleted']")
    private WebElement successDeletedMessage;

    @FindBy(xpath = "//input[@data-placeholder='Name']")
    private WebElement nameInSearch;

    @FindBy(xpath = "(//span[@class='mat-button-wrapper'])[9]")
    private WebElement searchButton;

    @FindBy(xpath = "(//span[@class='mat-option-text'])[3]")
    private WebElement everyone;

    @FindBy(xpath = "(//span[@class='mat-button-wrapper'])[20]")
    private WebElement deleteInDialogWindow;

    @FindBy(xpath = "//div[text()='The Salary Type with Name \"QA Automation Engineer in Test\" already exists.']")
    private WebElement existedMessage;

    @FindBy(xpath = "(//fa-icon[@class='ng-fa-icon'])[16]")
    private WebElement closDialog;

    @FindBy(xpath = "//div[text()=' There is no data to display ']")
    private WebElement noDataToDisplay;

    public WebElement getNoDataToDisplay() {
        return noDataToDisplay;
    }

    public WebElement getClosDialog() {
        return closDialog;
    }

    public WebElement getExistedMessage() {
        return existedMessage;
    }

    public WebElement getSuccessDeletedMessage() {
        return successDeletedMessage;
    }

    public WebElement getEditedSuccessMessage() {
        return editedSuccessMessage;
    }

    public WebElement getDeleteInDialogWindow() {
        return deleteInDialogWindow;
    }

    public WebElement getEveryone() {
        return everyone;
    }

    public WebElement getHumRes() {
        return humRes;
    }

    public WebElement getSalary() {
        return salary;
    }

    public WebElement getSalaryType() {
        return salaryType;
    }

    public WebElement getPlusButton() {
        return plusButton;
    }

    public WebElement getEditButton() {
        return editButton;
    }

    public WebElement getDeleteButton() {
        return deleteButton;
    }

    public WebElement getInputName() {
        return inputName;
    }

    public WebElement getUserType() {
        return userType;
    }

    public WebElement getListBox() {
        return listBox;
    }

    public WebElement getSaveButton() {
        return saveButton;
    }

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getNameInSearch() {
        return nameInSearch;
    }

    public WebElement getSearchButton() {
        return searchButton;
    }
}