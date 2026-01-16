package commons;

import org.openqa.selenium.*;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.Color;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.List;
import java.util.Set;

public class BaseFactory {

    // WebDriver methods

    protected void openPage(WebDriver driver, String pageUrl) {
        driver.get(pageUrl);
    }

    protected String getPageUrl(WebDriver driver) {
        return driver.getCurrentUrl();
    }

    protected String getPageSource(WebDriver driver) {
        return driver.getPageSource();
    }

    protected String getPageTitle(WebDriver driver) {
        return driver.getTitle();
    }

    protected String getWindowId(WebDriver driver) {
        return driver.getWindowHandle();
    }

    protected Set<String> getAllWindowIds(WebDriver driver) {
        return driver.getWindowHandles();
    }

    protected void navigateBack(WebDriver driver) {
        driver.navigate().back();
    }

    protected void navigateForward(WebDriver driver) {
        driver.navigate().forward();
    }

    protected void refreshPage(WebDriver driver) {
        driver.navigate().refresh();
    }

    protected void navigateToUrl(WebDriver driver, String pageUrl) {
        driver.navigate().to(pageUrl);
    }

    private Alert waitForAlert(WebDriver driver) {
        return getExplicitWait(driver).until(ExpectedConditions.alertIsPresent());
    }

    protected void acceptAlert(WebDriver driver) {
        waitForAlert(driver).accept();
    }

    protected void dismissAlert(WebDriver driver) {
        waitForAlert(driver).dismiss();
    }

    protected String getAlertText(WebDriver driver) {
        return waitForAlert(driver).getText();
    }

    protected void sendKeysToAlert(WebDriver driver, String keysToSend) {
        waitForAlert(driver).sendKeys(keysToSend);
    }

    protected void switchToOtherWindowByCurrentId(WebDriver driver, String windowId) {
        Set<String> allWindowIds = getAllWindowIds(driver);
        if (allWindowIds.size() > 1) {
            for (String id : allWindowIds) {
                if (!id.equals(windowId)) {
                    driver.switchTo().window(id);
                    break;
                }
            }
        }
    }

    protected void switchToWindowByTitle(WebDriver driver, String windowTitle) {
        Set<String> allWindowIds = getAllWindowIds(driver);
        for (String id : allWindowIds) {
            driver.switchTo().window(id);
            if (getPageTitle(driver).equals(windowTitle)) {
                break;
            }
        }
    }

    protected void closeAllWindowsExceptWindowById(WebDriver driver, String windowId) {
        Set<String> allWindowIds = getAllWindowIds(driver);
        for (String id : allWindowIds) {
            if (!id.equals(windowId)) {
                driver.switchTo().window(id);
                driver.close();
            }
        }
        driver.switchTo().window(windowId);
    }

    // WebElement methods

    protected void clickElement(WebDriver driver, WebElement element) {
        waitForElementToBeClickable(driver, element).click();
    }

    protected void sendKeysToElement(WebDriver driver, WebElement element, String keysToSend) {
        WebElement e = waitForElementToBeVisible(driver, element);
        e.clear();
        e.sendKeys(keysToSend);
    }

    protected String getElementText(WebDriver driver, WebElement element) {
        return waitForElementToBeVisible(driver, element).getText();
    }

    protected String getElementAttribute(WebDriver driver, WebElement element, String attributeName) {
        return waitForElementToBeVisible(driver, element).getDomAttribute(attributeName);
    }

    protected String getElementProperty(WebDriver driver, WebElement element, String propertyName) {
        return waitForElementToBeVisible(driver, element).getDomProperty(propertyName);
    }

    protected String getElementCssValue(WebDriver driver, WebElement element, String propertyName) {
        return waitForElementToBeVisible(driver, element).getCssValue(propertyName);
    }

    protected String getElementColorHex(WebDriver driver, WebElement element, String propertyName) {
        return Color.fromString(getElementCssValue(driver, element, propertyName)).asHex().toUpperCase();
    }

    protected int getElementCount(WebDriver driver, List<WebElement> elements) {
        return waitForAllElementsToBeVisible(driver, elements).size();
    }

    protected boolean isElementDisplayed(WebDriver driver, WebElement element) {
        return waitForElementToBeVisible(driver, element).isDisplayed();
    }

    protected boolean isElementEnabled(WebDriver driver, WebElement element) {
        return waitForElementToBeVisible(driver, element).isEnabled();
    }

    protected boolean isElementSelected(WebDriver driver, WebElement element) {
        return waitForElementToBeVisible(driver, element).isSelected();
    }

    protected void selectOptionInDefaultDropdown(WebDriver driver, WebElement element, String optionValue) {
        new Select(waitForElementToBeClickable(driver, element)).selectByVisibleText(optionValue);
    }

    protected String getSelectedOptionInDefaultDropdown(WebDriver driver, WebElement element) {
        return new Select(waitForElementToBeVisible(driver, element)).getFirstSelectedOption().getText();
    }

    protected boolean isDefaultDropdownMultiple(WebDriver driver, WebElement element) {
        return new Select(waitForElementToBeVisible(driver, element)).isMultiple();
    }

    protected void checkDefaultCheckboxOrRadioButton(WebDriver driver, WebElement element) {
        WebElement e = waitForElementToBeClickable(driver, element);
        if (!e.isSelected()) {
            e.click();
        }
    }

    protected void uncheckDefaultCheckbox(WebDriver driver, WebElement element) {
        WebElement e = waitForElementToBeClickable(driver, element);
        if (e.isSelected()) {
            e.click();
        }
    }

    protected void switchToFrame(WebDriver driver, WebElement element) {
        getExplicitWait(driver).until(ExpectedConditions.frameToBeAvailableAndSwitchToIt(waitForElementToBeVisible(driver, element)));
    }

    protected void switchToDefaultContent(WebDriver driver) {
        driver.switchTo().defaultContent();
    }

    // Actions methods

    protected void moveToElement(WebDriver driver, WebElement element) {
        new Actions(driver).moveToElement(waitForElementToBeVisible(driver, element)).perform();
    }

    protected void clickAndHoldOnElement(WebDriver driver, WebElement element) {
        new Actions(driver).clickAndHold(waitForElementToBeVisible(driver, element)).perform();
    }

    protected void releaseMouseFromElement(WebDriver driver, WebElement element) {
        new Actions(driver).release(waitForElementToBeVisible(driver, element)).perform();
    }

    protected void rightClickOnElement(WebDriver driver, WebElement element) {
        new Actions(driver).contextClick(waitForElementToBeVisible(driver, element)).perform();
    }

    protected void doubleClickOnElement(WebDriver driver, WebElement element) {
        new Actions(driver).doubleClick(waitForElementToBeVisible(driver, element)).perform();
    }

    protected void pressKeyOnElement(WebDriver driver, WebElement element, Keys key) {
        new Actions(driver).sendKeys(waitForElementToBeVisible(driver, element), key).perform();
    }

    // JavascriptExecutor methods

    protected Object executeJS(WebDriver driver, String javascript) {
        return ((JavascriptExecutor) driver).executeScript(javascript);
    }

    protected void scrollToTopByJS(WebDriver driver) {
        ((JavascriptExecutor) driver).executeScript("window.scrollBy(0,-document.body.scrollHeight);");
    }

    protected void scrollToBottomByJS(WebDriver driver) {
        ((JavascriptExecutor) driver).executeScript("window.scrollBy(0,document.body.scrollHeight);");
    }

    protected void scrollElementIntoViewTopByJS(WebDriver driver, WebElement element) {
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", waitForElementToBeVisible(driver, element));
    }

    protected void scrollElementIntoViewBottomByJS(WebDriver driver, WebElement element) {
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(false);", waitForElementToBeVisible(driver, element));
    }

    protected void clickElementByJS(WebDriver driver, WebElement element) {
        ((JavascriptExecutor) driver).executeScript("arguments[0].click();", waitForElementToBeClickable(driver, element));
    }

    protected String getElementValidationMessageByJS(WebDriver driver, WebElement element) {
        return (String) ((JavascriptExecutor) driver).executeScript("return arguments[0].validationMessage;", waitForElementToBeVisible(driver, element));
    }

    protected void setElementAttributeByJS(WebDriver driver, WebElement element, String attributeName, String attributeValue) {
        ((JavascriptExecutor) driver).executeScript("arguments[0].setAttribute('" + attributeName + "', '" + attributeValue + "');", waitForElementToBeVisible(driver, element));
    }

    protected void removeElementAttributeByJS(WebDriver driver, WebElement element, String attributeName) {
        ((JavascriptExecutor) driver).executeScript("arguments[0].removeAttribute('" + attributeName + "');", waitForElementToBeVisible(driver, element));
    }

    protected void highlightElementByJS(WebDriver driver, WebElement element) {
        WebElement e = waitForElementToBeVisible(driver, element);
        JavascriptExecutor jsExecutor = (JavascriptExecutor) driver;
        String originalStyle = (String) jsExecutor.executeScript("return arguments[0].getAttribute('style') || '';", e);
        jsExecutor.executeScript("arguments[0].setAttribute('style', 'border: 2px solid red; border-style: dashed;');", e);
        sleepForSeconds(oneSecond);
        jsExecutor.executeScript("arguments[0].setAttribute('style', arguments[1]);", e, originalStyle);
    }

    protected boolean isImageLoadedByJS(WebDriver driver, WebElement element) {
        Object result = ((JavascriptExecutor) driver).executeScript("return arguments[0] && arguments[0].complete && arguments[0].naturalWidth > 0;", waitForElementToBeVisible(driver, element));
        return Boolean.TRUE.equals(result);
    }

    // Wait methods

    protected void waitForPageReady(WebDriver driver) {
        try {
            getExplicitWait(driver).until(WebDriver -> (Boolean) ((JavascriptExecutor) driver).executeScript("return (typeof jQuery === 'undefined' || (jQuery.active === 0)) && document.readyState === 'complete';"));
        } catch (TimeoutException e) {
            throw new RuntimeException("Page load timeout exceeded!", e);
        }
    }

    protected WebElement waitForElementToBeVisible(WebDriver driver, WebElement element) {
        return getExplicitWait(driver).until(ExpectedConditions.visibilityOf(element));
    }

    protected List<WebElement> waitForAllElementsToBeVisible(WebDriver driver, List<WebElement> elements) {
        return getExplicitWait(driver).until(ExpectedConditions.visibilityOfAllElements(elements));
    }

    protected WebElement waitForElementToBeClickable(WebDriver driver, WebElement element) {
        return getExplicitWait(driver).until(ExpectedConditions.elementToBeClickable(element));
    }

    // Custom methods

    protected void selectOptionInCustomDropdown(WebDriver driver, WebElement dropdown, List<WebElement> allOptions, String expectedOption) {
        clickElement(driver, dropdown);
        sleepForSeconds(oneSecond);
        List<WebElement> options = waitForAllElementsToBeVisible(driver, allOptions);
        for (WebElement option : options) {
            if (getElementText(driver, option).trim().equals(expectedOption)) {
                clickElement(driver, option);
                break;
            }
        }
    }

    protected void sleepForSeconds(long timeInSeconds) {
        try {
            Thread.sleep(timeInSeconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private WebDriverWait getExplicitWait(WebDriver driver) {
        return new WebDriverWait(driver, Duration.ofSeconds(longTimeout));
    }

    // Common constants

    private final long longTimeout = GlobalConstants.LONG_TIMEOUT;
    private final long oneSecond = GlobalConstants.ONE_SECOND;

}