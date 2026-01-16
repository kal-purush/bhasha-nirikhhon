package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import utils.CommonMethods;

public class AddEmployeePage extends CommonMethods {

    @FindBy(id="menu_pim_addEmployee")
    public WebElement addEmployeeOption;

    @FindBy(xpath = "//div[@class='head']/h1")
    public WebElement panelHeader;

    @FindBy(xpath = "//input[@name='employeeId']")
    public WebElement employeeId;

    @FindBy(id = "firstName")
    public WebElement firstNameField;

    @FindBy(id = "middleName")
    public WebElement middleNameField;

    @FindBy(id = "lastName")
    public WebElement lastNameField;

    @FindBy(id = "btnSave")
    public WebElement saveButton;

    @FindBy(xpath = "//span[@for='firstName']")
    public WebElement firstNameErrorMsg;

    @FindBy(xpath = "//span[@for='lastName']")
    public WebElement lastNameErrorMsg;

    @FindBy(xpath = "//div[@class='message warning fadable']")
    public WebElement existingEmpIdErrorMsg;

    public AddEmployeePage(){
        PageFactory.initElements(driver,this);
    }
}