package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import utilities.Driver;

public class CreatePage {

    public CreatePage() {
        PageFactory.initElements(Driver.getDriver(), this);
    }

    @FindBy(xpath = "(//li/a/span)[4]")
    public WebElement contactsModule;


    @FindBy(xpath="//button[@class='btn btn-primary btn-sm o_form_button_save']")
    public WebElement saveBtn;
    @FindBy(xpath = "//button[@class = 'btn btn-primary btn-sm o-kanban-button-new btn-default']")
    public WebElement createButton;
    @FindBy(xpath = "//button[@class = 'btn btn-primary btn-sm o-kanban-button-new btn-default']")
    public WebElement editCreateButton;
    @FindBy(xpath = "//button[@class = 'btn btn-primary btn-sm o-kanban-button-new btn-default']")
    public WebElement indCreateButton;

    @FindBy(xpath = "//button[contains(text(),' Discard')]")
    public WebElement discardBtn;
    @FindBy(xpath = "//button[@class='btn btn-primary btn-sm o_form_button_edit']")
    public WebElement editBtn;

    @FindBy(xpath = "/html[1]/body[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/div[2]/div[2]/input[1]")
    public WebElement companyRadioButton;

    @FindBy(xpath = "/html[1]/body[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/input[1]")
    public WebElement individualRadioBtn;

    @FindBy(xpath = "//input[@placeholder='C\uFEFFo\uFEFFm\uFEFFp\uFEFFa\uFEFFn\uFEFFy']")
    public WebElement indCompanyNameBox;
    @FindBy(xpath="//input[@placeholder='Name']")
    public WebElement contactName;

    @FindBy(xpath="//div[@name='parent_id']//span[@class='o_dropdown_button']")
    public WebElement indCompanyNameDropDownBtn;

    @FindBy(linkText = "Create and Edit")
    public WebElement indCreateAndEditCompany;

    @FindBy(xpath = "/html[1]/body[1]/ul[1]/li[4]")
    public WebElement indCompanySelection;

    @FindBy(xpath="(//table[@class='o_group o_inner_group o_group_col_6']//select[@class='o_input o_field_widget'])[1]")
    public WebElement contactAddressTypeBox;

    @FindBy(xpath= "//input[@placeholder='Street...']")
    public WebElement contactAddressStreet;

    @FindBy(xpath = "//input[@placeholder='City']")
    public WebElement contactAddressCity;

    @FindBy(xpath="//input[@placeholder='S\uFEFFt\uFEFFa\uFEFFt\uFEFFe']")
    public WebElement addressStateBox;

    @FindBy(xpath = "//html[1]/body[1]/ul[10]/li[1]/a[1]")

     public  WebElement indAddressStateSelection;

    @FindBy(xpath = "/html[1]/body[1]/ul[2]/li[5]")
    public WebElement companyStateSelection;

    @FindBy(xpath="//input[@placeholder='ZIP']")
    public WebElement contactAddressZip;

    @FindBy (xpath = "//input[@placeholder='C\uFEFFo\uFEFFu\uFEFFn\uFEFFt\uFEFFr\uFEFFy']")
    public WebElement contactCountry;

    @FindBy(xpath = "//html[1]/body[1]/ul[11]/li[1]")
    public WebElement countrySelection;



    @FindBy(xpath="//input[@placeholder=\'e.g. Sales Director\']")
    public WebElement jobPosition;

    @FindBy(xpath="//input[@name='phone']")
    public WebElement contactPhone;

    @FindBy(xpath="//input[@name='email']")
    public WebElement contactEmail;

    @FindBy(xpath ="//input[@name='website']")
    public WebElement contactWebsite;

    @FindBy(xpath = "//div[@name='title']//input[@class='o_input ui-autocomplete-input']")
   public WebElement ContactTitleBox;

   @FindBy(xpath = "//html[1]/body[1]/ul[3]/li[2]")
   public  WebElement ContactTitleSelection;

    @FindBy(xpath = "//button[@class='btn btn-sm btn-primary']")
    public  WebElement alert;




}