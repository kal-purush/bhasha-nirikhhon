package Pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

public class EditSparePage extends ParentPage{
	@FindBy(id = "spares_spareName")
	private WebElement spareNameInput;
	
	@FindBy(id = "spares_spareType")
	private WebElement spareTypeDropDown;
	
	@FindBy(name = "add")
	private WebElement buttonCreate;
	@FindBy(name = "delete")
	private WebElement buttonDelete;
	
	public EditSparePage(WebDriver driver) {
		super(driver);
	}
	
	public void enterTextInToSpareNameInput(String spareName){
		actionsWithOurElement.enterText(spareNameInput,spareName);
	}
	
	public void selectTextInDropDownTypeOfSpare(String typeOfSpareForSelect){
		actionsWithOurElement.selectTextInDropDown(spareTypeDropDown, typeOfSpareForSelect);
	}
	
	public void clickButtonCreate(){
		actionsWithOurElement.clickOnElement(buttonCreate);
	}
	
	public void clickOnButtonDelete(){
		actionsWithOurElement.clickOnElement(buttonDelete);
	}
}