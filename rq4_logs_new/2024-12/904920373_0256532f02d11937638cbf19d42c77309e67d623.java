package elements;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import utils.SetUpClass;

public class CommansElements extends SetUpClass {
    public WebElement EnableButton = driver.findElement(By.xpath("//*[@id=\"input-example\"]/button"));
    public WebElement Input = driver.findElement(By.xpath("//*[@id=\"input-example\"]/input"));
 //   public WebElement Text = driver.findElement(By.xpath("//*[@id=\"message\"]"));

}