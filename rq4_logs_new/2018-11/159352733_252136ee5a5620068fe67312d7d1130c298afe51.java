package Example;

import Utilities.BeforeAndAfter;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;

public class UploadFile extends BeforeAndAfter {
    private String filepath = System.getProperty("user.dir") + "/src/test/java/resources/UploadTest.txt";


    @Test
    public void uploadFile() {
        wait = new WebDriverWait(driver, 20);
        File file = new File(filepath);
        String path = file.getAbsolutePath();
        driver.manage().window().maximize();
        driver.get("http://the-internet.herokuapp.com/upload");
        wait.until(ExpectedConditions.elementToBeClickable(By.id("file-upload")));
        driver.findElement(By.id("file-upload")).sendKeys(path);
        driver.findElement(By.id("file-submit")).click();
        wait.until(ExpectedConditions.presenceOfElementLocated(By.id("uploaded-files")));
        String text = driver.findElement(By.id("uploaded-files")).getText();

    }
}