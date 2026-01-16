package homeworks.homework24;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProductsInTheFile extends WebDriverInit {
    @Test
    public void findElements() throws IOException {
        driver.get("https://rozetka.com.ua/");
        WebElement laptopsAndComputerCategory =
                driver.findElement(By.xpath("//ul[contains(@class, 'menu-categories_type_main')]/li[1]"));
        laptopsAndComputerCategory.click();
        WebElement subCategory = driver.findElement(By.xpath("(//a[contains(@href, 'c80004/')])[1]"));
        subCategory.click();
        Map<String, String> productTitlesWithPrices = new HashMap<>();
        List<WebElement> listOfTitles = driver.findElements(By.xpath("//span[@class='goods-tile__title']"));
        List<WebElement> listOfPrices = driver.findElements(By.xpath("//span[contains(@class,'price-value')]"));
        for (int i = 0; i < listOfTitles.size(); i++) {
            String productTitle = listOfTitles.get(i).getText().replaceAll("/.*", "");
            String productPrice = listOfPrices.get(i).getText().replaceAll("[^\\d ]", "");
            productTitlesWithPrices.put(productTitle, productPrice);
        }
        FileWriter fileWithLaptopsTitlesAndPrices = new FileWriter("src/test/java/homeworks/homework24/rozetkaTest.txt");
        for (Map.Entry<String, String> map : productTitlesWithPrices.entrySet()) {
            fileWithLaptopsTitlesAndPrices.write(map.getKey() + " - " + map.getValue() + "\n");
        }
        fileWithLaptopsTitlesAndPrices.close();
    }
}