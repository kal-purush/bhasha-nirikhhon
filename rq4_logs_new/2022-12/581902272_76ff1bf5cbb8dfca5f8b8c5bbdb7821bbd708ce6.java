package day09_actions;

import org.junit.Assert;
import org.junit.Test;
import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import utilities.TestBase;

import javax.swing.*;
import java.util.Set;

public class C01_MouseActions extends TestBase {

    @Test
    public void test01(){
        //1- Yeni bir class olusturalim: MouseActions1
        //2- https://the-internet.herokuapp.com/context_menu sitesine gidin
        driver.get("https://the-internet.herokuapp.com/context_menu");
        //3- Cizili alan uzerinde sag click yapin
        Actions actions=new Actions(driver);

        WebElement ciziliAlanElementi= driver.findElement(By.xpath("//div[@id='hot-spot']"));

        actions.contextClick(ciziliAlanElementi).perform();

        //4- Alert’te cikan yazinin “You selected a context menu” oldugunu test edin.
        String expectedAlertYazisi="You selected a context menu";
        String actualAlertYazisi=driver.switchTo().alert().getText();

        Assert.assertEquals(expectedAlertYazisi,actualAlertYazisi);

        //5- Tamam diyerek alert’i kapatalim
        driver.switchTo().alert().accept();

        //6- Elemental Selenium linkine tiklayalim
        String ilkSaydaWHD=driver.getWindowHandle();
        System.out.println(ilkSaydaWHD);
         driver.findElement(By.linkText("Elemental Selenium"));
        //7- Acilan sayfada h1 taginda “Elemental Selenium” yazdigini test edelim

        Set<String> ikiSayfaninWHDSeti=driver.getWindowHandles();
        String ikinciSayfaWHD="";
        for (String eachWHD:ikiSayfaninWHDSeti
             ) {
            if (!eachWHD.equals(ilkSaydaWHD)){
                ikinciSayfaWHD=eachWHD;
            }

        }
        driver.switchTo().window(ikinciSayfaWHD);
        String actualH1TagYazisi=driver.findElement(By.tagName("h1")).getText();
        String expectedH1TagYazisi="Elemental Selenium";

        Assert.assertEquals(actualH1TagYazisi,expectedH1TagYazisi);

    }
}