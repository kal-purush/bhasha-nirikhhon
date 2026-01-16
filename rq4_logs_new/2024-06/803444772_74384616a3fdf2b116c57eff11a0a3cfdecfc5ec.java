package Pages;

import Utilities.GWD_OmerFatih;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class DialogContentOmerFatih extends ParentPageOmerFatih {
    public DialogContentOmerFatih() {
        PageFactory.initElements(GWD_OmerFatih.getDriver(),this);
    }
    @FindBy(css="[formcontrolname='username']")
    public WebElement username;
    @FindBy(css="input[formcontrolname='password']")
    public WebElement password;

    @FindBy(css="button[aria-label='LOGIN']")
    public WebElement loginButton;
    @FindBy(xpath="//span[contains(text(),'Welcome')]")
    public WebElement confirm;
    @FindBy(xpath="//div[@class='ng-star-inserted']/img")
    public WebElement seeLogo;
    @FindBy(xpath="(//span[@class='mdc-button__label']/fa-icon)[1]")
    public WebElement courseTab;
    @FindBy(xpath="(//span[@class='mdc-button__label']/fa-icon)[2]")
    public WebElement calendarTab;
    @FindBy(xpath="(//span[@class='mdc-button__label']/fa-icon)[3]")
    public WebElement attendanceTab;
    @FindBy(xpath="(//span[@class='mdc-button__label']/fa-icon)[4]")
    public WebElement assingmentsTab;
    @FindBy(xpath="(//span[@class='mdc-button__label']/fa-icon)[5]")
    public WebElement gradingTab;
    @FindBy(xpath="//span[contains(text(),'Welcome')]")
    public WebElement courseLabel;
    @FindBy(xpath="(//span[@class='mdc-tab__text-label'])[1]")
    public WebElement calendarLabel;
    @FindBy(xpath="(//span[contains(text(),'Attendance')])[3]")
    public WebElement attendaceLabel;
    @FindBy(xpath="(//span[contains(text(),'Assignments')])[3]")
    public WebElement assignmentsLabel;
    @FindBy(xpath="(//span[contains(text(),'Grading')])[3]")
    public WebElement gradingLabel;
    @FindBy(xpath="//*[@class='svg-inline--fa fa-bars fa-fw']")
    public WebElement hamburger;
    @FindBy(xpath=" //span[contains(text(),'Messaging')]")
    public WebElement hoverOver;
    @FindBy(xpath=" //span[contains(text(),'New Message')]")
    public WebElement newMessage;
    @FindBy(xpath=" //span[contains(text(),'Inbox')]")
    public WebElement Inbox;
    @FindBy(xpath=" //span[contains(text(),'Outbox')]")
    public WebElement Outbox;
    @FindBy(xpath=" //span[contains(text(),'Trash')]")
    public WebElement Trash;
    @FindBy(xpath="//input[@id='mat-mdc-chip-list-input-0'] ")
    public WebElement Receivers;
    @FindBy(xpath="//button[@class='mat-mdc-tooltip-trigger mat-badge mdc-icon-button mat-mdc-icon-button mat-badge-accent mat-basic mat-mdc-button-base mat-badge-above mat-badge-after mat-badge-small mat-badge-hidden ng-star-inserted']")
    public WebElement AddReceivers;
    @FindBy(xpath="//ms-confirm-button[@class='ng-star-inserted']")
    public WebElement Movetotrash;
    @FindBy(xpath="//strong[contains(text(),'Confirm')]")
    public WebElement Confirm;
    @FindBy(xpath="//span[contains(text(),'Yes')]")
    public WebElement yesButton;
    @FindBy(xpath="(//ms-delete-button[@class='ng-star-inserted'])[1]")
    public WebElement deleteButton;
    @FindBy(xpath="//ms-standard-button[@icon='trash-restore']")
    public WebElement restoreButton;
    @FindBy(xpath="(//div[@class='mat-expansion-panel-body ng-tns-c857250080-107'])[1]")
    public WebElement successMessage;
    @FindBy(xpath="//Strong[contains(text(),'Delete')]")
    public WebElement deleteConfirm;
    @FindBy(xpath="//button[@class='mdc-button mat-mdc-button mdc-button--raised mat-mdc-raised-button mat-accent mat-mdc-button-base']")
    public WebElement deletefinish;
    @FindBy(xpath="//input[@placeholder='Name, Username or E-mail']")
    public WebElement fieldReceiver;
    @FindBy(id="mat-mdc-checkbox-3")
    public WebElement selectReceiver;
    @FindBy(xpath="//input[@id='ms-text-field-0']")
    public WebElement subjectEntry;
    @FindBy(xpath="//span[contains(text(),'Add & Close' )]")
    public WebElement addClose;
    @FindBy(id="tinymce")
    public WebElement iframetext;
    @FindBy(xpath="//div[contains(text(),'Image...')]")
    public WebElement addImage;
    @FindBy(xpath="//span[contains(text(),'Finance')]")
    public WebElement financeLink;
    @FindBy(xpath="//span[contains(text(),'My Finance')]")
    public WebElement myfinanceLink;
    @FindBy(xpath="//span[contains(text(),' Students Fees ')]")
    public WebElement studentsFees;
    @FindBy(xpath="//span[contains(text(),' Student ID: 106')]")
    public WebElement studentsId;
    @FindBy(xpath="//span[contains(text(),'Fee/Balance Detail')]")
    public WebElement freeBalance;
    @FindBy(xpath="//div[contains(text(),'Installment')]")
    public WebElement Installment;
    @FindBy(xpath="//div[contains(text(),'Copy')]")
    public WebElement Copy;
    @FindBy(xpath = "//span[contains(text(),'Attach Files')]")
    public WebElement attachFileBtn;
    @FindBy(css = "ms-standard-button[icon=\"hdd\"]")
    public WebElement fromLocalButton;

    @FindBy(xpath = "//div[contains(text(),'Message Successfully sent')]")
    public WebElement sentMessage;
    @FindBy(css = "ms-button[caption$=SEND]")
    public WebElement sendButton;



















































    public WebElement getWebElement(String strElement){

        switch (strElement){


        }

        return null;
    }
}
