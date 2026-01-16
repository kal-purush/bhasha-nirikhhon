package homeworks.homework23;

import initWebDriverTest.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;

public class ProductInTheBasket extends WebDriverInit {

    final String EXPECTED_PTODUCT_QUANTITY = "1";
    final String EXPECTED_TITTLE = "Ноутбук Lenovo IdeaPad Slim 5 16IAH8 (83BG001ARA) Cloud Grey / 16\" IPS WUXGA / " +
            "Intel Core i5-12450H / RAM 16 ГБ / SSD 512 ГБ / Підсвічування клавіатури";

    @Test
    public void addingProductToTheBasket() {
        driver.get("https://rozetka.com.ua/");
        WebElement computersAndNotebooksSection =
                driver.findElement(By.xpath("//li[@class='menu-categories__item ng-star-inserted']" +
                        "/following::a[contains(@href, 'computers-notebooks/c80253')]"));
        computersAndNotebooksSection.click();
        WebElement notebooksSection = driver.findElement(By.xpath("//a[contains(@href, " +
                "'notebooks/c80004/')][contains(@class, 'tile-cats__picture')]"));
        notebooksSection.click();
        WebElement buyBtn = driver.findElement((By.xpath("(//button[contains(@class, 'buy-button')])[1]")));
        webDriverWait.until(ExpectedConditions.elementToBeClickable(buyBtn));
        buyBtn.click();
        WebElement basketProductsCounter = driver.findElement(
                By.xpath("//span[contains(@class, 'badge badge')]"));
        String sumOfProductsInTheBasket = basketProductsCounter.getAttribute("innerText");
        if (sumOfProductsInTheBasket.equals(EXPECTED_PTODUCT_QUANTITY)) {
            WebElement basketBtn = driver.findElement(
                    By.xpath("//button[@class= 'header__button ng-star-inserted header__button--active']"));
            basketBtn.click();
            WebElement productTitleInTheBasket = driver.findElement(
                    By.xpath("//a[@class='cart-product__title']"));
            String productTitleInTheBasketText = productTitleInTheBasket.getAttribute("innerText");
            Assert.assertEquals(productTitleInTheBasketText, EXPECTED_TITTLE, "Titles doesn’t equals");
        } else {
            fail("Product isn't added to the basket");
        }
    }
}