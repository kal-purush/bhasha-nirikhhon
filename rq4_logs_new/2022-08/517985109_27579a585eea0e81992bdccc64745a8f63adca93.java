package frontend.pages;

import frontend.Utils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import java.time.Duration;
import java.util.List;
import static frontend.Utils.waitSeconds;

public class cartPage {

    WebDriver driver;
    Utils utils;
    By purchaseBtn = By.cssSelector("button.btn.btn-success");
    By txtName = By.id("name");
    By txtCountry = By.id("country");
    By txtCity = By.id("city");
    By txtCreditCard = By.id("card");
    By txtMonth = By.id("month");
    By txtYear = By.id("year");
    By purchaseOrderBtn = By.xpath("//button[contains(@onclick,'purchaseOrder')]");
    By okBtn = By.cssSelector("button.confirm.btn.btn-lg.btn-primary");

    public void deleteLaptop() {
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(5));
        List<WebElement> rows = wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector("tbody#tbodyid tr")));
        table:
        for (WebElement cells : rows) {
            List<WebElement> productName = cells.findElements(By.tagName("td"));
            for (WebElement t : productName) {
                if (t.getAttribute("innerHTML").contains("Dell i7 8gb")) {
                    WebElement deleteBtn = cells.findElement(By.linkText("Delete"));
                    deleteBtn.click();
                    waitSeconds(2);
                    break table;
                }
            }
        }
    }

    public cartPage (WebDriver driver) {
        this.driver = driver;
        utils = new Utils(this.driver);
    }

    public void clickPlaceOrderBtn() {
        utils.waitUntil(purchaseBtn);
        driver.findElement(purchaseBtn).click();
    }

    public void enterName(String name) {
        utils.waitUntil(txtName);
        driver.findElement(txtName).sendKeys(name);
    }

    public void enterCountry(String country) {
        driver.findElement(txtCountry).sendKeys(country);
    }

    public void enterCity(String city) {
        driver.findElement(txtCity).sendKeys(city);
    }

    public void enterCreditCard(String card) {
        driver.findElement(txtCreditCard).sendKeys(card);
    }

    public void enterMonth(String month) {
        driver.findElement(txtMonth).sendKeys(month);
    }

    public void enterYear(String year) {
        driver.findElement(txtYear).sendKeys(year);
    }

    public void clickPurchaseOrder() {
        utils.waitUntil(purchaseOrderBtn);
        driver.findElement(purchaseOrderBtn).click();
    }

    public void clickOk() {
        utils.waitUntil(okBtn);
        driver.findElement(okBtn).click();
    }
}