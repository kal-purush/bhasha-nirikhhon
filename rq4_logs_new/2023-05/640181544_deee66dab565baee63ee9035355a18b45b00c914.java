package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.util.List;

public class ItemCategories_POM extends MyMethods {

    public ItemCategories_POM() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "(//span[text()='Inventory'])[1]")
    private WebElement inventory;

    @FindBy(xpath = "(//span[text()='Setup'])[4]")
    private WebElement setup;

    @FindBy(xpath = "//span[text()='Item Categories']")
    private WebElement itemCategories;

    @FindBy(xpath = "//ms-add-button[@table='true']")
    private WebElement addButton;

    @FindBy(xpath = "//ms-edit-button[@table='true']")
    private WebElement editButton;

    @FindBy(xpath = "//ms-delete-button[@table='true']")
    private WebElement deleteButton;

    @FindBy(xpath = "(//input[@data-placeholder='Name'])[2]")
    private WebElement name;

    @FindBy(xpath = "//mat-chip-list[@formcontrolname='roles']")
    private WebElement userType;

    @FindBy(xpath = "//ms-save-button[@class='ng-star-inserted']")
    private WebElement saveButton;

    @FindBy(xpath = "//button[@type='submit']")
    private WebElement deleteConfirm;

    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessage;

    @FindBy(xpath = "//div[contains(text(),'already')]")
    private WebElement warningMessage;

    @FindBy(xpath = "//mat-error[text()=' This field is required!']")
    private WebElement requiredMessage;

    @FindBy(xpath = "//div[@role='listbox']")
    private List<WebElement> userTypeOptions;

    public WebElement getInventory() {
        return inventory;
    }

    public WebElement getSetup() {
        return setup;
    }

    public WebElement getItemCategories() {
        return itemCategories;
    }

    public WebElement getAddButton() {
        return addButton;
    }

    public WebElement getEditButton() {
        return editButton;
    }

    public WebElement getDeleteButton() {
        return deleteButton;
    }

    public WebElement getName() {
        return name;
    }

    public WebElement getUserType() {
        return userType;
    }

    public WebElement getSaveButton() {
        return saveButton;
    }

    public WebElement getDeleteConfirm() {
        return deleteConfirm;
    }

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getWarningMessage() {
        return warningMessage;
    }

    public WebElement getRequiredMessage() {
        return requiredMessage;
    }

    public List<WebElement> getUserTypeOptions() {
        return userTypeOptions;
    }
}