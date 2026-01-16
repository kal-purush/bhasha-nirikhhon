package Pages;

import Utilities.GWD;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class US002Pages extends Parent{
    public US002Pages() {PageFactory.initElements(GWD.getDriver(), this);
    }
    @FindBy(xpath="//*[text()='Human Resources']")
    public WebElement humanBtn;
    @FindBy(xpath="(//*[text()='Setup'])[3]")
    public WebElement setupBtn;
    @FindBy(xpath="(//*[text()='Attestations'])[1]")
    public WebElement attestationsBtn;
    @FindBy(xpath=" //input[@id='ms-text-field-0']")
    public WebElement nameSearch;
    @FindBy(xpath="//span[text()='Search']")
    public WebElement search;
    @FindBy(xpath="//ms-add-button[contains(@tooltip,'ADD')]//button")
    public WebElement addBtn;
    @FindBy(xpath="//ms-text-field[@formcontrolname='name']//input")
    public WebElement name;
    @FindBy(xpath="//*[@class='hot-toast-close-btn ng-star-inserted']")
    public WebElement errorMsg;
    @FindBy(xpath="//mat-form-field//input[@data-placeholder='Name']")
    public WebElement searchInput;
    @FindBy(xpath="//ms-search-button//button")
    public WebElement searchBtn;
    @FindBy(xpath="(//ms-delete-button//button)[1]")
    public WebElement deleteImageBtn;
    @FindBy(xpath="//button[@type='submit']")
    public WebElement deleteDialogBtn;
    @FindBy(xpath=" //ms-save-button/button")
    public WebElement save;
    @FindBy(xpath="//ms-edit-button/button")
    public WebElement editBtn;
    @FindBy(xpath="//div[contains(text(),'successfully')]")
    public WebElement successMessage;



    public WebElement getWebElement(String strElement){
        switch (strElement){
            case "humanBtn": return this.humanBtn;
            case "setupBtn": return this.setupBtn;
            case "attestationsBtn": return this.attestationsBtn;
            case "nameSearch": return this.nameSearch;
            case "search": return this.search;
            case "addBtn": return this.addBtn;
            case "name": return this.name;
            case "save": return this.save;
            case "errorMsg": return this.errorMsg;
            case "searchInput": return this.searchInput;
            case "searchBtn": return this.searchBtn;
            case "deleteImageBtn": return this.deleteImageBtn;
            case "deleteDialogBtn": return this.deleteDialogBtn;
            case "editBtn": return this.editBtn;
            case "successMessage": return this.successMessage;


        }

        return null;
    }










}