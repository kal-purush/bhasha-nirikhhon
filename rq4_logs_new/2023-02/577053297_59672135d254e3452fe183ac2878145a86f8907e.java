package arcadia.pages;
import arcadia.pages.ComponentDB.AddNewComponentPage;
import arcadia.utils.SeleniumCustomCommand;
import com.aventstack.extentreports.cucumber.adapter.ExtentCucumberAdapter;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.FindBy;
import org.testng.Assert;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;


public class ConnectorEditorPage extends BasePage{

    @FindBy(css = "#cEditor table.htCore") private WebElement tableConnectorEditor;

    String connectorEditorRows = "#cEditor table.htCore tbody tr";

    SeleniumCustomCommand customCommand = new SeleniumCustomCommand();
    public ConnectorEditorPage(WebDriver driver) {
        super(driver);
    }

    public void verifyConnectorEditorOpened(){
        customCommand.waitForElementVisibility(driver,tableConnectorEditor);
        customCommand.waitForElementToBeClickable(driver,tableConnectorEditor);
        Assert.assertTrue(tableConnectorEditor.isDisplayed(),"Connector editor is not opened");
    }

//    public void enterConnectorDetails(String conID, String compDB, String partType, String partNumber, String description) {
//        This method is not complete yet.
//        List<WebElement> connectorEditorTableRows = driver.findElements(By.cssSelector(connectorEditorRows));
//        WebElement row3 = connectorEditorTableRows.get(2);
//        List<WebElement> tdElements = row3.findElements(By.cssSelector("td"));
//        customCommand.waitForElementToBeClickable(driver,tdElements.get(0));
//        customCommand.doubleClick(driver,tdElements.get(0));
//        tdElements.get(0).sendKeys(conID);
//        tdElements.get(3).sendKeys(compDB);
//        tdElements.get(4).sendKeys(partType);
//        tdElements.get(4).sendKeys(partNumber);
//        tdElements.get(6).sendKeys(description);
//    }
}