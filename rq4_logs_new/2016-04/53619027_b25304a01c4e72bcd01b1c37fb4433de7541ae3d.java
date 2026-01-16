package testing;

import ecommerce.NameUtils;
import ecommerce.Product;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Desafío 3 - obtener toda la lista de productos
 * Esto suena a robar información, y casi que lo es. La propuesta es grabar un caso de prueba que sea capaz de recorrer todos los productos del sistema y obtenga el dato del nombre del producto, y lo guarde a un archivo.
 * Este archivo lo usaremos después para el último desafío.
 */

public class Desafio3Test {

    FirefoxDriver wd;
    WebElement weNom, weDes, wePri;
    Product p;
    Integer iter=1, page=1;
    Integer maxIter = 16, maxPage=3;
    Double price =0d;
    ArrayList productos = new ArrayList<Product>(50);
    Product product;

    @Before
    public void setUp() throws Exception {
        wd = new FirefoxDriver();
        wd.manage().timeouts().implicitlyWait(60, TimeUnit.SECONDS);
    }

    @Test
    public void testDesafio3(){

        while (page < maxPage) {
            wd.get("http://demo.opencart.com/index.php?route=product/search&search=&page="+page);

//            (new WebDriverWait(wd, 5000l)).until(new ExpectedCondition<Boolean>() {
//                public Boolean apply(WebDriver d) {
//                    return d.getPageSource().contains("Search -");
//                }
//            });


            while (iter < maxIter) {
                try {
                    weNom = wd.findElement(By.xpath("//div[@id='content']/div[4]/div[" + iter + "]/div/div[2]/h4/a"));
                    wePri = wd.findElement(By.xpath("//div[@id='content']/div[4]/div[" + iter + "]/div/div[2]/p[2]/span[1]"));
                    weDes = wd.findElement(By.xpath("//div[@id='content']/div[4]/div[" + iter + "]/div/div[2]/p[1]"));

                    product = new Product(
                            weNom.getText(),
                            NameUtils.parse(wePri.getText(), Locale.US).doubleValue(),
                            weDes.getText());
                    System.out.println(product);
                    productos.add(product);

                }catch (NoSuchElementException ex) {}
                catch (ParseException pe) {}
                iter++;
            }
            page++;
        }
    }


    @After
    public void tearDown() throws Exception {
        wd.close();
        wd.quit();
    }
}