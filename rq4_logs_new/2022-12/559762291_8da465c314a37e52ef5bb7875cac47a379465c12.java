package page_library;

import base.BasePage;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;

import java.util.List;

public class BuildVehicle extends BasePage {

    @FindBy(xpath = "//nav[@id ='StudioNavigation']//a")
    public List<WebElement> studioNavList;

    @FindBy(xpath = "//div[@id ='byo-studio-rail-design']/div[1]/div")
    public List<WebElement> designList;

    @FindBy(xpath = "//div[@id = 'byo-studio-rail-exterior']//div[contains(@class,'byo-rail-option-base')]")
    public List<WebElement> exteriorColorList;

    @FindBy(xpath = "//div[@class = 'build-overview__vehicle']/div[@class = 'build-overview__vehicle-section']/div/div[1]")
    public List<WebElement> summaryOverview;

    @FindBy(xpath = "//div[@class = 'flex-container']/div")
    public List<WebElement> exteriorWheelsList;

    @FindBy(xpath = "//input[@id ='Wheels_selector']")
    public WebElement wheelsButton;

    @FindBy(xpath = "//div[@class = 'byo-rail-list-view-items']//div[contains(@class,'byo-rail-option-base')]")
    public List<WebElement> interiorOptionsList;

    @FindBy(xpath = "(//div[@class = 'option-tile-image'])[1]")
    public WebElement sedansFeaturedPackageButton;

    @FindBy(xpath = "//*[@id='Studio']/div[1]/div/div[1]/div[1]/div/div[1]/div/div[2]/div[1]/div[2]/div[1]/button")
    public WebElement sedansFeaturedPackageSelectedButton;

    @FindBy(css = "#Trim_selector")
    public WebElement interiorTrimButton;

    @FindBy(xpath = "//button[@class = 'package-modal__selected-btn label-1']")
    public WebElement premiumAddToBuildButton;

    @FindBy(xpath = "//button[@class = 'package-modal__selected-btn label-1 package-modal__selected-btn--active']")
    public WebElement premiumRemoveFromBuildButton;

    @FindBy(xpath = "//button[@name = 'confirm-button']")
    public WebElement confirmChangesButton;

    @FindBy(xpath = "//ul[@class = 'byo-featured-packages__list']/li[1]//div[@class='package-tile__modal-btn-wrapper']")
    public WebElement premiumSeeALlFeaturesButton;

    public BuildVehicle() {
        PageFactory.initElements(driver, this);
    }


    public void customizeVehicle(String section) {
        fluentWait.until(ExpectedConditions.visibilityOfAllElements(studioNavList));
        WebElement ele;
        switch (section) {
            case "Design":
                ele = fluentWait.until(ExpectedConditions.visibilityOf(studioNavList.get(0)));
                safeClickOnElement(ele);
                break;
            case "Exterior":
                ele = fluentWait.until(ExpectedConditions.visibilityOf(studioNavList.get(1)));
                safeClickOnElement(ele);
                break;
            case "Interior":
                ele = fluentWait.until(ExpectedConditions.visibilityOf(studioNavList.get(2)));
                safeClickOnElement(ele);
                break;
            case "Options":
                ele = fluentWait.until(ExpectedConditions.visibilityOf(studioNavList.get(3)));
                safeClickOnElement(ele);
                break;
            case "Summary":
                ele = fluentWait.until(ExpectedConditions.visibilityOf(studioNavList.get(4)));
                safeClickOnElement(ele);
                break;
            default:
                System.out.println("Invalid section!!!");
        }
    }

    public String selectDesign(String index) {
        String title = "";
        WebElement ele;
        int num = Integer.parseInt(index);
        if (num < 0 || num > designList.size()) {
            System.out.println("Invalid index!!!");
        } else {
            ele = designList.get(num);
            title = ele.getAttribute("title");
            clickOnElement(ele);
        }
        return title;
    }

    public String selectExteriorColor(String index) {
        String title = "";
        WebElement ele;
        int num = Integer.parseInt(index);
        try {
            int listSize = webDriverWait.until(ExpectedConditions.visibilityOfAllElements(exteriorColorList)).size();
            if (num < 0 || num > listSize) {
                System.out.println("Invalid index!!!");
            } else {
                ele = fluentWait.until(ExpectedConditions.visibilityOf(exteriorColorList.get(num)));
                title = ele.getAttribute("title");
                safeClickOnElement(ele);
            }
        } catch (StaleElementReferenceException e) {
            int listSize = webDriverWait.until(ExpectedConditions.visibilityOfAllElements(exteriorColorList)).size();
            if (num < 0 || num > listSize) {
                System.out.println("Invalid index!!!");
            } else {
                ele = fluentWait.until(ExpectedConditions.visibilityOf(exteriorColorList.get(num)));
                title = ele.getAttribute("title");
                safeClickOnElement(ele);
            }
        }
        return title;
    }

    public String selectExteriorWheelsType(String index) {
        safeClickOnElement(wheelsButton);
        String title = "";
        WebElement ele;
        int num = Integer.parseInt(index);
        int listSize = webDriverWait.until(ExpectedConditions.visibilityOfAllElements(exteriorWheelsList)).size();
        if (num < 0 || num > listSize) {
            System.out.println("Invalid index!!!");
        } else {
            ele = fluentWait.until(ExpectedConditions.visibilityOf(exteriorWheelsList.get(num)));
            title = ele.getAttribute("title");
            safeClickOnElement(ele);
        }
        return title;
    }

    public String customizeInterior(String index, boolean isTrim) {
        if (isTrim) {
            safeClickOnElement(interiorTrimButton);
        }
        String title = "";
        WebElement ele;
        int num = Integer.parseInt(index);
        try {
            int listSize = webDriverWait.until(ExpectedConditions.visibilityOfAllElements(interiorOptionsList)).size();
        } catch (StaleElementReferenceException e) {
            int listSize = webDriverWait.until(ExpectedConditions.visibilityOfAllElements(interiorOptionsList)).size();
            if (num < 0 || num > listSize) {
                System.out.println("Invalid index!!!");
            } else {
                ele = fluentWait.until(ExpectedConditions.visibilityOf(interiorOptionsList.get(num)));
                title = ele.getAttribute("title");
                safeClickOnElement(ele);
            }
        }

        return title;
    }

    public void selectFeaturedPackageToBuild() {
        scrollByVisibleElement(fluentWait.until(ExpectedConditions.visibilityOf(premiumSeeALlFeaturesButton)));
        jsClickOnElement(premiumSeeALlFeaturesButton);
        safeClickOnElement(fluentWait.until(ExpectedConditions.visibilityOf(premiumAddToBuildButton)));
    }

    public void removeFeaturedPackageFromBuild() {
        safeClickOnElement(fluentWait.until(ExpectedConditions.visibilityOf(premiumRemoveFromBuildButton)));
        if(isElementVisible(fluentWait.until(ExpectedConditions.visibilityOf(confirmChangesButton)))) {
            safeClickOnElement(fluentWait.until(ExpectedConditions.visibilityOf(confirmChangesButton)));
        }
    }


    public String verifyBuild(int index) {
        String text = "";
        WebElement ele;
        int size = fluentWait.until(ExpectedConditions.visibilityOfAllElements(summaryOverview)).size();
        if (index < 0 || index > size) {
            System.out.println("Invalid index!!!");
        } else {
            ele = fluentWait.until(ExpectedConditions.visibilityOf(summaryOverview.get(index)));
            text = ele.getText();
        }
        return text;
    }


}