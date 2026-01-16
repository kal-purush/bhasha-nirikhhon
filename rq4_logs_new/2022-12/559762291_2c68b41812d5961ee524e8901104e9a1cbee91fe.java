package page_library;

import base.BasePage;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;

import java.time.Month;
import java.util.List;

import static java.lang.Integer.parseInt;

public class ContactsPage extends BasePage {

    @FindBy(xpath = "//a[@href='/contacts/new']")
    public WebElement createNewContactButton;

    @FindBy(name = "first_name")
    public WebElement firstNameInputBox;

    @FindBy(name = "last_name")
    public WebElement lastNameInputBox;

    @FindBy(css = "input[name='do_not_text']")
    public WebElement doNotTextButton;

    @FindBy(xpath = "//div[@class='ui small icon input']/input[@name = 'day']")
    public WebElement dayOfBirthInputBox;

    @FindBy(xpath = "//div[@class ='visible menu transition']//div[@class = 'item']")
    public List<WebElement> allMonths;

    @FindBy(xpath = "//div[@name = 'month']")
    public WebElement monthOfBirthButton;

    @FindBy(xpath = "//input[@name ='year']")
    public WebElement yearOfBirthInputBox;

    @FindBy(xpath = "//button[@class = 'ui linkedin button' ]")
    public WebElement saveContactButton;

    @FindBy(xpath = "//*[@id='dashboard-toolbar']/div[1]")
    public static WebElement newlyCreatedContactName;

    @FindBy(xpath = "//div[@class = 'ui form segment' ]/div[21]")
    public static WebElement actBirthday;

    @FindBy(xpath = "//*[@id='main-content']/div/div[2]/div/table/tbody/tr/td[2]")
    public static WebElement contactsName;

    @FindBy(xpath = "//*[@id='main-content']/div/div[2]/div/table/tbody/tr/td[2]/div//span")
    public WebElement noRecordsFoundElement;

    @FindBy(xpath = "//*[@id='dashboard-toolbar']/div[2]/div/a[1]")
    public static WebElement editContactButton;

    @FindBy(xpath = "//div[@name ='category' and @role ='listbox']")
    public WebElement categoryButton;

    @FindBy(xpath = "//div[@class ='visible menu transition']//div[@name ='category'][3]")
    public WebElement contactCategory;

    @FindBy(xpath = "//*[@id='main-content']/div/div[2]/div/div[1]/div/div[6]")
    public WebElement actCategorySelected;

    @FindBy(xpath = "//button[@class ='ui button icon']")
    public WebElement contactPageDeleteButton;

    @FindBy(xpath = "//button[@class ='ui red button']")
    public WebElement confirmDeletionButton;

    @FindBy(xpath = "//button[@class='ui basic button item']")
    public WebElement uiDeleteIcon;

    @FindBy(xpath = "//div[@class='ui menu']/a[1]")
    public WebElement contactsDeletedButton;

    @FindBy(xpath = "//*[@id='main-content']/div/div[2]/div/div[3]/table/tbody/tr[1]/td[2]")
    public WebElement lastDeletedContact;

    @FindBy(xpath = "//tbody//div[@class = 'ui fitted checkbox' ]")
    public WebElement lastDeletedContactCheckBox;

    @FindBy(xpath = "//button[@class = 'ui button']")
    public WebElement restoreSelectedButton;

    @FindBy(xpath = "//button[@class = 'ui primary button']")
    public WebElement confirmRestoreButton;

    @FindBy(xpath = "//button[@class ='ui negative button']")
    public WebElement purgeSelectedButton;

    @FindBy(xpath = "//button[@class ='ui primary button']")
    public WebElement confirmPurgeButton;

    public ContactsPage() {
        PageFactory.initElements(driver, this);
    }

    private void clickCreateContactButton() {
        clickOnElement(createNewContactButton);
    }

    private void sendKeysFirstName(String firstName) {
        sendKeysToElement(firstNameInputBox, firstName);
    }

    private void sendKeysLastName(String lastName) {
        sendKeysToElement(lastNameInputBox, lastName);
    }

    private void clickDoNotTextButton() {
        elementBeforeAndAfterCSSTag("input[name='do_not_text']");
        jsClickOnElement(doNotTextButton);
    }

    private void sendKeysDayOfBirth(String day) {
        sendKeysToElement(dayOfBirthInputBox, day);
    }

    private void clickMonthOfBirthButton() {
        clickOnElement(monthOfBirthButton);
    }

    private void selectMonthOfBirth(String month) {
        int myMonth = Month.valueOf(month.toUpperCase()).getValue();
        webDriverWait.until(ExpectedConditions.visibilityOfAllElements(allMonths));
        if (myMonth <= allMonths.size()) {
            clickOnElement(allMonths.get(myMonth - 1));
        } else {
            System.out.println("Invalid month!!!");
        }
    }

    private void sendKeysYearOfBirth(String year) {
        sendKeysToElement(yearOfBirthInputBox, year);
    }

    public void clickSaveContactButton() {
        clickOnElement(saveContactButton);
    }

    public void createNewContact(String fName, String lName, String day, String month, String year) {
        clickCreateContactButton();
        sendKeysFirstName(fName);
        sendKeysLastName(lName);
        scrollAtTheBottomOfPage();
        clickDoNotTextButton();
        sendKeysDayOfBirth(day);
        clickMonthOfBirthButton();
        selectMonthOfBirth(month);
        sendKeysYearOfBirth(year);
        clickSaveContactButton();
    }

    public ContactsPage clickEditContactButton(){
        clickOnElement(editContactButton);
        return new ContactsPage();
    }

   public void selectContactCategory(){
        clickOnElement(categoryButton);
        clickOnElement(contactCategory);
    }

    public void clickContactPageDeleteButton(){
        clickOnElement(contactPageDeleteButton);
    }
    public void clickConfirmDeletion(){clickOnElement(confirmDeletionButton);}


}
