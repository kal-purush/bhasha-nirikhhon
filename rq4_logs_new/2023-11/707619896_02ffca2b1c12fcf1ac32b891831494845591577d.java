package lessons.lesson26;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import static java.lang.Thread.sleep;

public class TestFrameExample extends WebDriverInit {

    @Test
    public void testFrame() throws InterruptedException {
        driver.get("https://the-internet.herokuapp.com/iframe");
        WebElement frame = driver.findElement(By.id("mce_0_ifr"));

        driver.switchTo().frame(frame);
        WebElement inputInFrame = driver.findElement(By.id("tinymce"));
        sleep(3000);
        inputInFrame.clear();
        inputInFrame.sendKeys("Hello world, I'm here");
        sleep(5000);

    }
}