package pageObjects;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.io.File;

public class UploadPage extends BasePage {

    @FindBy(css = "input[type='file']")
    private WebElement fileInput;

    @FindBy(id = "file-submit")
    private WebElement submitButton;

    public UploadPage() {
        PageFactory.initElements(webDriver, this);
    }

    public UploadPage setFile(File pathName) {
        fileInput.sendKeys(pathName.getAbsolutePath());
        return this;
    }

    public UploadResultPage submitButton() {
        submitButton.click();
        return new UploadResultPage();
    }
}