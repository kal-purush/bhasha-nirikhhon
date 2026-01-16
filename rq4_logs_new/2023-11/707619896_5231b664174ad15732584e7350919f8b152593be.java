package homeworks.homework26;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.lang.Thread.sleep;

public class CheckingAvailabilityOfPromoProductsSelectedByFilter extends WebDriverInit {

    final String EXPECTED_PROMO_LABEL = "АКЦІЯ";

    @Test
    public void displayOfPromoStickers() throws InterruptedException {
        openLaptopsPage();
        selectSupplier();
        setMaxPrice();
        findPromoProduct();
        openProductPageWithPromoSticker();
        changeCFCookie("2eeImjDdy07qf4fLS0jtP13u5u8l3ItEEhCkfWVlYhM-1701020941-0-1-a0523e9a.8c804b95.f753617a-0.2.1701020941");
        checkPromoStickerOnProductPage();
    }

    /*public void openMainPage() throws InterruptedException {
        driver.get("https://rozetka.com.ua/ua/");
        sleep(5000);
    }

    public void goToLaptopSubcategory() {
        WebElement laptopsAndComputerCategory = webDriverWait.until(ExpectedConditions.elementToBeClickable(By
                .xpath("//ul[contains(@class, 'menu-categories_type_main')]/li[1]")));
        laptopsAndComputerCategory.click();
        WebElement subCategory = webDriverWait.until(ExpectedConditions.elementToBeClickable(By
                .xpath("(//a[contains(@href, 'c80004/')])[1]")));
        subCategory.click();
    }*/

    public void openLaptopsPage() {
            driver.get("https://rozetka.com.ua/ua/notebooks/c80004/");
        }
    public void selectSupplier() {
        WebElement supplierCheckbox = driver.findElement(By.xpath("//a[@data-id='Rozetka']"));
            supplierCheckbox.click();
    }

    public void setMaxPrice() {
        WebElement maxPriceInput = driver.findElement(By.xpath("//input[@formcontrolname='max']"));
        maxPriceInput.clear();
        maxPriceInput.sendKeys("100000");
        maxPriceInput.submit();
    }

    public void findPromoProduct() {
        WebElement productWithPromoLabel = webDriverWait.until(ExpectedConditions.presenceOfElementLocated(By
                .xpath("//span[contains(@class, 'type_action')]")));
        String labelOnProduct = productWithPromoLabel.getText().trim();
        Assert.assertEquals(labelOnProduct, EXPECTED_PROMO_LABEL, "Product with promo is absent");
    }

    public void openProductPageWithPromoSticker() {
        WebElement productWithPromoLabelLinkOnProductPage = webDriverWait.until(ExpectedConditions.elementToBeClickable(By
                .xpath("//span[contains(@class, 'type_action')]/../a")));
        String productPageLink = productWithPromoLabelLinkOnProductPage.getAttribute("href");
        driver.get(productPageLink);
    }

    public void changeCFCookie(String value) {
        Cookie cookie = new Cookie("cf_clearance", value);
        driver.manage().deleteCookieNamed("cf_clearance");
        driver.manage().addCookie(cookie);
        driver.navigate().refresh();
    }

    public void checkPromoStickerOnProductPage() {
        WebElement productPromoSticker = driver.findElement(By
                .xpath("(//span[contains(@class, 'action promo-label')])[2]"));
        String promoLabelOnProductPage = productPromoSticker.getText().trim();
        Assert.assertEquals(promoLabelOnProductPage, EXPECTED_PROMO_LABEL, "Label is absent");
    }
}