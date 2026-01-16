import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.Select;

public class Desafio1 {
	@Test
	public void testeTextField() {
		WebDriver driver = new FirefoxDriver();
		driver.get("file:///" + System.getProperty("user.dir") + "/src/main/resources/componentes.html");
		driver.findElement(By.id("elementosForm:nome")).sendKeys("Matheus");
		//Assert.assertEquals("Matheus", driver.findElement(By.id("elementosForm:nome")));
		driver.findElement(By.id("elementosForm:sobrenome")).sendKeys("Santos");
		//Assert.assertEquals("Santos", driver.findElement(By.id("elementosForm:sobrenome")));
		
		
	
		driver.findElement(By.id("elementosForm:sexo:0")).click();
		//Assert.assertTrue(driver.findElement(By.id("elementosForm:sexo:0")).isSelected());
		
	
		driver.findElement(By.id("elementosForm:comidafavorita:2")).click();
		//Assert.assertTrue(driver.findElement(By.id("elementosForm:comidafavorita:2")).isSelected());
		
	
		WebElement element = driver.findElement(By.id("elementosForm:escolaridade"));
		Select combo = new Select(element);
		combo.selectByIndex(3);
			
	
		driver.findElement(By.id("elementosForm:sugestoes")).sendKeys("teste");
		//Assert.assertEquals("teste",driver.findElement(By.id("elementosForm:sugestoes")).getAttribute("value"));
		
	
		
	    
		//WebElement elements = driver.findElement(By.id("elementosForm:esportes"));
		//Select combo = new Select(elements);
		//combo.selectByVisibleText("Natacao");
		//combo.selectByVisibleText("Corrida");
		//combo.selectByVisibleText("O que eh esporte?");
		
	//	List<WebElement> allSelectedOptions = combo.getAllSelectedOptions();
		
		
	
	
	
	
	

		driver.findElement(By.id("elementosForm:cadastrar")).click();
		//driver.quit();
	}
}