package id.ac.ui.cs.advprog.eshop.functional;

import io.github.bonigarcia.seljup.SeleniumJupiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeDriver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ExtendWith(SeleniumJupiter.class)
public class CreateProductFunctionalTest {
    /**
     * The port number assigned to the running application during test execution.
     * Set automatically during each test run by Spring Framework's test context.
     */
    @LocalServerPort
    private int serverPort;

    /**
     * The base URL for testing. Default value to {@code http://localhost}.
     */
    @Value("${app.baseUrl:http://localhost}")
    private String testBaseUrl;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = String.format("%s:%d", testBaseUrl, serverPort);
    }

    // Test menuju halaman Create Product
    @Test
    void createProductPage_isCorrect(ChromeDriver driver) throws Exception {
        // Exercise
        driver.get(baseUrl + "/product/create");
        String pageTitle = driver.getTitle();

        // Verify
        assertEquals("Create New Product", pageTitle);
    }

    // Test untuk cek keberhasilkan dalam Create Product
    @Test
    void createProductPost_isCorrect(ChromeDriver driver) throws Exception {
        // Exercise
        driver.get(baseUrl + "/product/create");
        driver.findElement(By.id("nameInput")).sendKeys("Sampo Cap Bambang");
        driver.findElement(By.id("quantityInput")).sendKeys("100");
        driver.findElement(By.id("submitButton")).click();
        String pageTitle = driver.getTitle();

        // Verify
        assertEquals("Product List", pageTitle);
    }

    // Test untuk cek keberhasilan pengalihan halaman ke Product List
    @Test
    void productListPage_isCorrect(ChromeDriver driver) throws Exception {
        // Exercise
        driver.get(baseUrl + "/product/list");
        String pageTitle = driver.getTitle();

        // Verify
        assertEquals("Product List", pageTitle);
    }

    // Test untuk cek apakah product yang tadi dibuat berhasil ditampilkan
    @Test
    void createdProduct_isCorrect(ChromeDriver driver) throws Exception {
        // Exercise
        driver.get(baseUrl + "/product/list");
        String productName = driver.findElement(By.xpath("//tbody/tr[1]/td[1]")).getText();
        String productQuantity = driver.findElement(By.xpath("//tbody/tr[1]/td[2]")).getText();

        // Verify
        assertEquals("Sampo Cap Bambang", productName);
        assertEquals("100", productQuantity);
    }
}