package arcadia.pages.ComponentDB;

import arcadia.pages.BasePage;
import arcadia.utils.SeleniumCustomCommand;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

import java.util.List;

public class ComponentDBHomePage extends BasePage {
    SeleniumCustomCommand customCommand = new SeleniumCustomCommand();

    public ComponentDBHomePage(WebDriver driver) {
        super(driver);
    }

    @FindBy(css = "form#idCloneForm input[name=\"newLibraryID\"]") private WebElement inputFieldNewComponentDB;

    @FindBy(css = "form#idCloneForm input[name=\"version\"]") private WebElement inputFieldNewComponentDBVersion;

    @FindBy(css = "form#idCloneForm button#btnClone") private WebElement buttonCloneComponent;

    public WebElement getButtonCloneComponentDB(String dbName){
        WebElement ele = driver.findElement(By.cssSelector("div#idavailablelibrary a[href$=\"database="+dbName+"\"]+div.btns>button[title=\"Clone Component DB\"]"));
        return ele;
    }
    public Boolean checkDBExists(String dbName){
        List<WebElement> listOfElements = driver.findElements(By.cssSelector("div#idavailablelibrary a[href$=\"database="+dbName+"\"]"));
        if (listOfElements.size()==1){
            return true;
        }
        else {
            return false;
        }
    }

    public void cloneComponentDB(String componentDBToBeCloned, String newDbName) throws InterruptedException {
        WebElement ele = driver.findElement(By.cssSelector("div#idavailablelibrary a[href$=\"database="+componentDBToBeCloned+"\"]"));
        customCommand.mouseHover(driver,ele);
        customCommand.javaScriptClick(driver,getButtonCloneComponentDB(componentDBToBeCloned));
        customCommand.waitForElementVisibility(driver,inputFieldNewComponentDB);
        customCommand.waitForElementToBeClickable(driver,inputFieldNewComponentDB);
        customCommand.enterText(inputFieldNewComponentDB,newDbName);
        customCommand.enterText(inputFieldNewComponentDBVersion,"1");
        buttonCloneComponent.click();
    }
}