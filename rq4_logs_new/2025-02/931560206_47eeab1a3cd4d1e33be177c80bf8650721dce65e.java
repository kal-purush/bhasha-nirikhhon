package id.ac.ui.cs.advprog.eshop.functional;

import io.github.bonigarcia.seljup.SeleniumJupiter;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.WebElement;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ExtendWith(SeleniumJupiter.class)
class CreateProductFunctionalTest {
    @LocalServerPort
    private int serverPort;

    @Value("${app.baseUrl:http://localhost}")
    private String testBaseUrl;

    private String baseUrl;

    @BeforeEach
    void setupTest() {
        baseUrl = String.format("%s:%d/product/create", testBaseUrl, serverPort);
    }

    @Test
    void pageTitle_isCorrect(ChromeDriver driver) throws Exception {
        driver.get(baseUrl);
        String pageTitle = driver.getTitle();
        assertEquals("Create New Product", pageTitle);
    }

    @Test
    void message_createProduct_isCorrect(ChromeDriver driver) throws Exception {
        driver.get(baseUrl);
        String message = driver.findElement(By.tagName("h3")).getText();
        assertEquals("Create New Product", message);
    }

    @Test
    void createProduct_isCorrect(ChromeDriver driver) throws Exception {
        driver.get(baseUrl);
        String expectedProductName = "Laptop ASUS TUF Dash F15";
        int expectedProductQuantity = 50;

        WebElement nameInputField = driver.findElement(By.id("nameInput"));
        nameInputField.sendKeys(expectedProductName);

        WebElement quantityInputField = driver.findElement(By.id("quantityInput"));
        quantityInputField.sendKeys(String.valueOf(expectedProductQuantity));

        WebElement submitButton = driver.findElement(By.tagName("button"));
        submitButton.click();

        String listUrl = driver.getCurrentUrl();
        assertEquals(String.format("%s:%d/product/list", testBaseUrl, serverPort), listUrl);

        assertFalse(driver.findElements(By.xpath("//*[contains(text(), '" + expectedProductName + "')]")).isEmpty());
        assertFalse(driver.findElements(By.xpath("//*[contains(text(), '" + expectedProductQuantity + "')]")).isEmpty());
    }
}