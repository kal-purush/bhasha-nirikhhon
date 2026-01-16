package org.automation.framework;

import org.openqa.selenium.By;
import org.openqa.selenium.ElementNotInteractableException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.List;

public class SeleniumActions {

    private final BrowserManager browserManager;

    public SeleniumActions(BrowserManager browserManager) {
        this.browserManager = browserManager;
    }

    public boolean isElementDisplayed(By locator) {
        return browserManager.getDriver().findElement(locator).isDisplayed();
    }

    public boolean isElementEnabled(By locator) {
        return browserManager.getDriver().findElement(locator).isEnabled();
    }

    public List<WebElement> getElements(By locator) {
        return browserManager.getDriver().findElements(locator);
    }

    public void clickElement(By locator) {
        browserManager.getDriver().findElement(locator).click();
    }

    public String getElementText(By locator) {
        return browserManager.getDriver().findElement(locator).getText();
    }

    public void sendKeys(By locator, String text) {
        browserManager.getDriver().findElement(locator).sendKeys(text);
    }

    public void waitElementToBeClickable(By locator, int timeOut) {
        Wait<WebDriver> wait = new WebDriverWait(browserManager.getDriver(), Duration.ofSeconds(timeOut));
        wait.until(ExpectedConditions.elementToBeClickable(locator));
    }

    public void waitFluentElementClickable(By locator, int timeOut) {
        Wait<WebDriver> wait = new FluentWait<>(browserManager.getDriver())
                .withTimeout(Duration.ofSeconds(timeOut))
                .pollingEvery(Duration.ofMillis(300))
                .ignoring(ElementNotInteractableException.class);
        wait.until(ExpectedConditions.elementToBeClickable(locator));
    }

    public void waitFluentElementVisible(By locator, int timeOut) {
        Wait<WebDriver> wait = new FluentWait<>(browserManager.getDriver())
                .withTimeout(Duration.ofSeconds(timeOut))
                .pollingEvery(Duration.ofMillis(300))
                .ignoring(ElementNotInteractableException.class);
        wait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(locator));
    }

    public void waitToSpinnerToHide(By locator, int timeToWait) {
        Wait<WebDriver> wait = new WebDriverWait(browserManager.getDriver(), Duration.ofSeconds(timeToWait));
        wait.until(ExpectedConditions.invisibilityOfElementLocated(locator));
    }
}