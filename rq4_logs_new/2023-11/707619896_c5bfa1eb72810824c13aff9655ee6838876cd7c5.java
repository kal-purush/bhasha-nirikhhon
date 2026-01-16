package lessons.lesson24;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.SortedSet;

import static java.lang.Thread.sleep;

public class TestFindElements extends WebDriverInit {

    @Test
    public void findElements() throws InterruptedException {
        driver.get("https://rozetka.com.ua/");
        WebElement laptopsandComputerCategory =
                driver.findElement(By.xpath("//ul[contains(@class, 'menu-categories_type_main')]/li[1]"));
        laptopsandComputerCategory.click();
        sleep(5000);
        WebElement subCategory = driver.findElement(By.xpath("(//a[contains(@href, 'c80004/')])[1]"));
        subCategory.click();
        sleep(5000);
        List<WebElement> listOfTitles = driver.findElements(By.xpath("//span[@class='goods-tile__title']"));
        for (WebElement element: listOfTitles) {
            String titleText = element.getText();
            System.out.println(titleText);
        }
        Assert.assertEquals(listOfTitles.size(), 60, "Elements quantity on site does not equal 60");
    }

}