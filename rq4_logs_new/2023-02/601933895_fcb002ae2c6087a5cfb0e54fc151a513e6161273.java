package stepdefinitions;

import java.util.List;
import java.util.Map;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.Select;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.*;

public class Registrationstep extends BaseTest {
 WebDriver driver;
 
 public Registrationstep() {
	 this.driver=super.driver;
 }
	
	
	@When("User Enters Following user details with coloums")
	public void user_enters_following_user_details_with_coloums(DataTable dataTable) {
	    List<Map<String , String>> data=dataTable.asMaps(String.class,String.class);
	    driver.findElement(By.id("nf-field-17")).sendKeys(data.get(0).get("Firstname"));
	    driver.findElement(By.id("nf-field-18")).sendKeys(data.get(0).get("Lastname"));
	    driver.findElement(By.id("nf-field-19")).sendKeys(data.get(0).get("Email"));
	    driver.findElement(By.id("nf-field-20")).sendKeys(data.get(0).get("Phone"));
	    
	    
	    // Write code here that turns the phrase above into concrete actions
	    // For automatic transformation, change DataTable to one of
	    // E, List<E>, List<List<E>>, List<Map<K,V>>, Map<K,V> or
	    // Map<K, List<V>>. E,K,V must be a String, Integer, Float,
	    // Double, Byte, Short, Long, BigInteger or BigDecimal.
	    //
	    // For other transformations you can register a DataTableType.
	    
	}
	@When("Select the Course you would like to book")
	public void select_the_course_you_would_like_to_book() {
		Select drpDown=new Select(driver.findElement(By.id("nf-field-22")));
		drpDown.selectByValue("mobile-automation");
	   
	}
	@When("Select the Month of the Batch you would like to join")
	public void select_the_month_of_the_batch_you_would_like_to_join() {
	  Select monthDropdown=new Select( driver.findElement(By.id("nf-field-24")));
	  monthDropdown.selectByValue("april");
	  
	}
	@When("How do you know about us ?")
	public void how_do_you_know_about_us() {
	   driver.findElement(By.xpath("//label[@id='nf-label-class-field-23-2']")).click();
	}
	@Then("User clicks on the Register button")
	public void user_clicks_on_the_register_button() {
	   driver.findElement(By.id("nf-field-15")).click();
	}

}