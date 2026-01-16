package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

import java.util.List;

public class SettingsPage extends AbstractPage{

    @FindBy(xpath = "//div[contains(@class, 'columns')]/div[contains(@class, 'column')]")
    private List<WebElement> columns;

    @FindBy(xpath = "//h1[contains(text(), 'Site Settings')]")
    private WebElement settingsTitle;

    @FindBy(xpath = "//input[contains(@name, 'Catalog - number of columns')]")
    private WebElement numberOfColumnsInput;

    public int changeNumberOfColumns(int number) {
        waitVisibility(settingsTitle);
        String numberRepresentation = String.valueOf(number);
        numberOfColumnsInput.sendKeys(numberRepresentation);
        submitButton.click();
        return columns.size();
    }
}