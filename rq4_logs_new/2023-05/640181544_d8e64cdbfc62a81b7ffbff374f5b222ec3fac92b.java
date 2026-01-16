package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class SalaryParameters_POM extends MyMethods {
    public SalaryParameters_POM() {PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "//span[text()='Human Resources']")
    private WebElement humanResourceButton;

    @FindBy(xpath = "(//span[text()='Salary'])[1]")
    private WebElement salaryButton;

    @FindBy(xpath = "(//span[text()='Salary Parameters'])[1]")
    private WebElement salaryParameters1Button;

    @FindBy(css = "svg[data-icon='plus']")
    private WebElement addButton;

    @FindBy(xpath = "//input[@name='name']")
    private WebElement nameButton;

    @FindBy(xpath = "//span[@class='mat-mdc-button-touch-target']")
    private WebElement validFrom;

    @FindBy(xpath = "//ms-date[@formcontrolname='fromDate']")
    private  WebElement validFromButton;

    @FindBy(xpath = "//span[text()=' 20 ']")
    private  WebElement date;

    @FindBy(xpath = "(//input[@data-placeholder='Key'])[2]")
    private WebElement keyButton;

    @FindBy(xpath = "//input[@data-placeholder='Value']")
    private WebElement valueButton;

    @FindBy(xpath = "//span[text()='Save']")
    private WebElement saveButton;

    @FindBy(css = "//ms-edit-button[@table='true']")
    private WebElement editButton;

    @FindBy(xpath = "//ms-delete-button[@table='true']")
    private WebElement deleteButton;

    @FindBy(xpath = "//button[@type='submit']")
    private WebElement deleteConfirmButton;

    @FindBy(xpath = "//div[contains(text(),'text')]")
    private WebElement warningMessage;

    @FindBy(xpath = "//mat-error[text()=' This field is required!']")
    private WebElement requiredErrorMessage;

    public WebElement getHumanResourceButton() {
        return humanResourceButton;
    }

    public WebElement getSalaryButton() {
        return salaryButton;
    }

    public WebElement getSalaryParameters1Button() {
        return salaryParameters1Button;
    }

    public WebElement getAddButton() {
        return addButton;
    }

    public WebElement getNameButton() {
        return nameButton;
    }

    public WebElement getValidFromButton() {
        return validFromButton;
    }

    public WebElement getKeyButton() {
        return keyButton;
    }

    public WebElement getValueButton() {
        return valueButton;
    }

    public WebElement getSaveButton() {
        return saveButton;
    }

    public WebElement getEditButton() {
        return editButton;
    }

    public WebElement getDeleteButton() {
        return deleteButton;
    }

    public WebElement getDeleteConfirmButton() {
        return deleteConfirmButton;
    }

    public WebElement getValidFrom() {
        return validFrom;
    }

    public WebElement getDate() {
        return date;
    }

    public WebElement getRequiredErrorMessage() {
        return requiredErrorMessage;
    }

    public WebElement getWarningMessage() {
        return warningMessage;
    }
}