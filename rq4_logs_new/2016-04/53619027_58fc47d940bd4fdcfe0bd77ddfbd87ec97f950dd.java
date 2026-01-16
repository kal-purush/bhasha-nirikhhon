package testing;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.firefox.FirefoxDriver;

import java.util.concurrent.TimeUnit;

/**
 * Grabar el siguiente caso de prueba

 Entrar en  OpenCart (demo.opencart.com).
 Buscar el producto "iPhone" (primero ingresar el nombre del producto, luego tocar el botón de la lupa)
 Verificar que el primer elemento que se muestra es el que corresponde a un iPhone.
 Luego que pudieron grabar y reproducir esto en el Selenium Builder, exportar el caso de prueba a código Java. Cargar ese código Java dentro de Eclipse, y asegurarse que funcione adecuadamente como una prueba JUnit.
 */
public class Desafio1Test {

    FirefoxDriver wd;

    @Before
    public void setUp() throws Exception {
        wd = new FirefoxDriver();
        wd.manage().timeouts().implicitlyWait(60, TimeUnit.SECONDS);
    }

    @Test
    public void testDesafio1(){

        wd.get("http://demo.opencart.com");
        wd.findElement(By.name("search")).click();
        wd.findElement(By.name("search")).clear();
        wd.findElement(By.name("search")).sendKeys("iphone");
        wd.findElement(By.xpath("//span[@class='input-group-btn']/button")).click();
        wd.findElement(By.id("grid-view")).click();
        wd.findElement(By.xpath("//div[@class='image']/a/img")).click();

        String nombre = wd.findElement(By.cssSelector("h1")).getText();

        Assert.assertEquals("Name first element differen","iphone",nombre.toLowerCase());

        wd.close();
        wd.quit();
    }
}