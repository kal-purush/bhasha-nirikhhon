package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class Ayse_POM extends MyMethods {
    public Ayse_POM() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    @FindBy(xpath = "//span[text()='Inventory']")
    private WebElement inventoryButton;
    @FindBy(xpath = "(//span[text()='Setup'])[4]")
    private WebElement setupButtonUnderInventory;

    @FindBy(xpath = "//ms-add-button[@table='true']")
    private WebElement addButton;
    @FindBy(xpath = "(//span[@class='mat-button-wrapper'])[16]")
    private WebElement addNewButton;
    @FindBy(xpath = "(//span[@class='mat-button-wrapper'])[17]")
    private WebElement saveButton;

    @FindBy(css = "input[data-placeholder='Name']")
    private WebElement inputNameBtn;
    @FindBy(xpath = "//ms-text-field[@formcontrolname='name']")
    private WebElement editNameBtn;
    @FindBy(xpath = "//span[text()='Item Category']")
    private WebElement itemCategory;
    @FindBy(xpath = "//div[contains(text(),'invalid')]")
    private WebElement warningMessage;
    @FindBy(xpath = "(//input[@type='text'])[2]")
    private WebElement defaultPrice;
    @FindBy(xpath = "//span[text()='Measurement Unit']")
    private WebElement measurementUnit;
    @FindBy(xpath = "//input[@data-placeholder='Barcode']")
    private WebElement barcode;
    @FindBy(xpath = "//*[@data-placeholder='Description']")
    private WebElement descriptionBox;

    @FindBy(xpath = "(//span[@class='mat-button-wrapper'])[17]")
    private WebElement saveBtn;
    @FindBy(xpath = "//span[text()=' Supplies ']")
    private WebElement selectItemCategory;
    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessage;

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getWarningMessage() {
        return warningMessage;
    }

    public WebElement getSelectItemCategory() {
        return selectItemCategory;
    }

    public WebElement getInputNameBtn() {
        return inputNameBtn;
    }

    public WebElement getItemCategory() {
        return itemCategory;
    }

    public WebElement getDefaultPrice() {
        return defaultPrice;
    }

    public WebElement getMeasurementUnit() {
        return measurementUnit;
    }

    public WebElement getBarcode() {
        return barcode;
    }

    public WebElement getDescriptionBox() {
        return descriptionBox;
    }

    public WebElement getSaveBtn() {
        return saveBtn;
    }

    public WebElement getInventoryButton() {
        return inventoryButton;
    }

    public WebElement getSetupButtonUnderInventory() {
        return setupButtonUnderInventory;
    }

    public WebElement getItemTypes() {
        return itemTypes;
    }

    @FindBy(xpath = "(//span[text()='Item Types'])[1]")
    private WebElement itemTypes;

    public WebElement getAddButton() {
        return addButton;
    }

    public WebElement getAddNewButton() {
        return addNewButton;
    }

    public WebElement getSaveButton() {
        return saveButton;
    }

    public WebElement getEditNameBtn() {
        return editNameBtn;
    }
}