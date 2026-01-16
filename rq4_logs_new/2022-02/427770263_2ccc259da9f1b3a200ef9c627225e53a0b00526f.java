package pageObjectsSelenide;

import com.codeborne.selenide.Condition;
import com.codeborne.selenide.SelenideElement;

import java.io.File;

import static com.codeborne.selenide.Selenide.$;

public class UploadPage {
    private final SelenideElement fileInput = $("input[type='file']");
    private final SelenideElement submitButton = $("#file-submit");
    private final SelenideElement uploadVerification = $("#content > div > h3");

    public void setFileInInput(File pathname) {
        fileInput.should(Condition.visible).should(Condition.enabled).sendKeys(pathname.getAbsolutePath());
    }

    public void submitFileUpload() {
        submitButton.should(Condition.visible).should(Condition.enabled).click();
    }

    public void uploadVerification() {
        uploadVerification.should(Condition.visible);
    }
}