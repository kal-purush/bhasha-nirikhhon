package Pages.Burak;

import Utilities.BasicDriver;
import Utilities.Parent_Burak;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.time.Duration;

public class DialogContent_Burak {


    public DialogContent_Burak() {
        PageFactory.initElements(BasicDriver.getDriver(), this);
    }

    Parent_Burak pb = new Parent_Burak();
    WebElement myElement;

    @FindBy(css = "svg[data-icon='plus']")
    private WebElement addBtn;

    @FindBy(css = "input[type='text']")
    private WebElement name;

    @FindBy(xpath = "//span[text()='Active']")
    private WebElement activeToggleBtn;

    @FindBy(xpath = "//div[text()=' Affect to Total ']")
    private WebElement affectToTotalToggleBtn;

    @FindBy(css = "textarea[id*=ms-textarea]")
    private WebElement descriptionArea;

    @FindBy(className = "mat-raised-button")
    private WebElement saveBtn;

    @FindBy(xpath = "//div[text()='Grade Excuse Type successfully created']")
    private WebElement successMessage;

    @FindBy(xpath = "//div[@class='hot-toast-bar-base']//dynamic-view/div")
    private WebElement unsuccessMessage;



    public void clickElementDialogContect(String strElement) {
        switch (strElement) {
            case "addBtn":
                myElement = addBtn;
                break;
            case "activeToggleBtn":
                myElement = activeToggleBtn;
                break;
            case "Affect to Total":
                myElement = affectToTotalToggleBtn;
                break;
            case "saveBtn":
                myElement = saveBtn;
        }
        pb.clickElement_Tools(myElement);
    }

    public void sendKeyDialogContent(String strElement, String text) {
        switch (strElement) {
            case "name":
                myElement = name;
                break;
            case "descriptionArea":
                myElement = descriptionArea;
                break;
        }
        pb.sendKeysElement_Tools(myElement, text);
    }

    public void verifyElementContainsText(String strElement, String text){
        switch (strElement){
            case "successMessage":
                myElement = successMessage;
                break;
            case "unsuccessMessage":
                myElement = unsuccessMessage;
        }
        pb.verifyElementContainsText_Tools(myElement,text);
    }
}