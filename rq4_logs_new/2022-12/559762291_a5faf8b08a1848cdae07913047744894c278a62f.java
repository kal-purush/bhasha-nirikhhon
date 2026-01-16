package page_libray;

import base.BasePage;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;

import java.util.List;

public class BuildVehicle extends BasePage {

    @FindBy(xpath = "//ul[contains(@id,'sticky-nav-links')]//a")
    public List<WebElement> navList;//size = 5;

    @FindBy(xpath = "//section[@class = 'all-vehicles module-separator']//section[2]//div[@class='all-vehicles__body-group-listing']/div/div/button")
    public List<WebElement> listOfSUVs;//size = 11;

    //section[@class = 'all-vehicles module-separator']//section[2]//div[@class='all-vehicles__body-group-listing']/div[3]/div/ul/li
    @FindBy(xpath = "//section[@class = 'all-vehicles module-separator']//section[3]//div[@class='all-vehicles__body-group-listing']/div/div/button")
    public List<WebElement> listOfSedansWagons;//size = 8;

    @FindBy(xpath = "//section[@class = 'all-vehicles module-separator']//section[4]//div[@class='all-vehicles__body-group-listing']/div/div/button")
    public List<WebElement> listOfCoupes;//size = 6

    @FindBy(xpath = "//section[@class = 'all-vehicles module-separator']//section[5]//div[@class='all-vehicles__body-group-listing']/div/div/button")
    public List<WebElement> listOfConvertibles;//size = 3;

    @FindBy(xpath = "//section[@class = 'all-vehicles module-separator']//section[6]//div[@class='all-vehicles__body-group-listing']/div/div/button")
    public List<WebElement> listOfElectric;//size = 4;

    @FindBy(xpath = "//button[@class = 'button button_primary button--wide' ]")
    public WebElement contMyBuildButton;

    @FindBy(xpath = "//button[@id = 'summary']")
    public WebElement summaryButton;

    @FindBy(xpath = "//h1[@class = 'mbs-build-summary-banner__model-name']")
    public WebElement summaryModelName;

    @FindBy(xpath = "//h1[@class = 'model-line-selector__heading-vehicle-name wrapper']")
    public WebElement headVehicleNameSelected;

    public BuildVehicle() {
        PageFactory.initElements(driver, this);
    }

    /*index 0 => SUVs
    index 1 => Sedans & Wagons
    index 2 => Coupes
    index 3 => Convertibles
    index 4 => Electric */

    public void selectVehicleType(String vehicleType) {
        switch (vehicleType) {
            case "SUVs":
                clickOnElement(navList.get(0));
                break;
            case "Sedans":
                clickOnElement(navList.get(1));
                break;
            case "Coupes":
                clickOnElement(navList.get(2));
                break;
            case "Convertibles":
                clickOnElement(navList.get(3));
            case "Electric":
                clickOnElement(navList.get(4));
            default:
                System.out.println("Invalid type!!!");
        }

    }

    public void clickContinueMyBuildButton() {
        clickOnElement(contMyBuildButton);
    }

    public void clickSummaryButton() {
        clickOnElement(summaryButton);
    }

    public String chooseSUVsModel(String index, String modelOption, String vehicleTypeIndex) {
        String expText = "";
        int vehicleIndex = Integer.parseInt(index);
        int option = Integer.parseInt(modelOption);
        int vehicleType = Integer.parseInt(vehicleTypeIndex);
        switch (vehicleType) {
            case 0:
                clickOnElement(navList.get(vehicleType));
                expText = selectVehicleModel(vehicleIndex, option,(vehicleType+2), listOfSUVs);
                break;
            case 1:
                clickOnElement(navList.get(vehicleType));
                expText = selectVehicleModel(vehicleIndex, option,(vehicleType+2), listOfSedansWagons);
                break;
            case 2:
                clickOnElement(navList.get(vehicleType));
                expText = selectVehicleModel(vehicleIndex, option,(vehicleType+2), listOfCoupes);
                break;
            case 3:
                clickOnElement(navList.get(vehicleType));
                expText = selectVehicleModel(vehicleIndex, option,(vehicleType+2), listOfConvertibles);
                break;
            case 4:
                clickOnElement(navList.get(vehicleType));
                expText = selectVehicleModel(vehicleIndex, option,(vehicleType+2), listOfElectric);
            default:
                System.out.println("Invalid index!!!");
        }
        return expText;
    }

    private String selectVehicleModel(int index, int option, int section, List<WebElement> elementList) {
        String expText = "";
        if (index < 0 || index >= elementList.size()) {
            System.out.println("Invalid index!!!" + " the size of the list is :" + elementList.size());
        } else {
            webDriverWait.until(ExpectedConditions.visibilityOfAllElements(elementList));
            safeClickOnElement(elementList.get(index));
            List<WebElement> models = driver.findElements
                    (By.xpath("//section[@class = 'all-vehicles module-separator']//section["+section+"]//" +
                            "div[@class='all-vehicles__body-group-listing']/div[" + (index + 1) + "]/div/ul/li/a"));
            webDriverWait.until(ExpectedConditions.visibilityOfAllElements(models));
            if (option <= 0 || option >= models.size()) {
                System.out.println("Invalid modelOption!!!" + " the size of the list is :" + models.size());
            } else {
                expText = models.get(option - 1).getText().trim();
                safeClickOnElement(models.get(option - 1));
            }
        }
        return expText;
    }


}