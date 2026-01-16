package pages;

import static common.CommonActions.*;

import java.util.List;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.Select;

import io.github.bonigarcia.wdm.WebDriverManager;
import io.opentelemetry.sdk.metrics.internal.view.DropAggregation;

public class EnrollNowInner {
	public WebDriver driver;
	Select select;

	public EnrollNowInner(WebDriver driver) {

		this.driver = driver;
		PageFactory.initElements(driver, this);
	}

	@FindBy(xpath = "//a[@name='login-link']")
	WebElement login;

	@FindBy(xpath = "//input[@name='username' and @id='emails']")
	WebElement userEmailElement;

//CSS Other attribute
	@FindBy(css = "input[name='password']")
	WebElement passwordCSSElement;

	@FindBy(xpath = "//input[@id='login']")
	WebElement loginSubmitElement;

	// @FindBy(xpath = "//li[@class='submenu active']") Compound class with Xpath Do
	// not Work.
	@FindBy(xpath = "//span[text()='Automation']")
	WebElement automationElement;

	// Inner Enroll Now
	@FindBy(xpath = "//a[text()='Enroll Now']")
	WebElement enrollNowInner;

	@FindBy(xpath = "//button[text()='Mouse Hover Action']")
	WebElement mouseHover;

	@FindBy(xpath = "//h3[contains(text(), 'Select')]")
	WebElement enrollNowInnerHeader;

	@FindBy(xpath = "//h5[contains(text(),'Please')]")
	WebElement enrollNowInnerSubHeader;

	@FindBy(xpath = "//p[contains(text(), 'All')]")
	WebElement enrollNowInnerOtherHeader;

	@FindBy(xpath = "//input[@name='f_name']")
	WebElement firstname;

	@FindBy(xpath = "//input[@name='m_name']")
	WebElement middName;

	@FindBy(xpath = "//input[@name='l_name' and @class='form-control']")
	WebElement lastName;

	@FindBy(xpath = "//select[@name='i_am']")
	WebElement selectProfession;

	@FindBy(xpath = "//select[@name='course_wish_to_enroll']")
	WebElement selectCourse;

	// @FindBy(xpath = "//select[@id='id_i_am']/option") works too
	@FindBy(xpath = "//select[@name='i_am']/option")
	List<WebElement> selectProfessionList;

	@FindBy(xpath = "//input[@name='phone']")
	WebElement phoneNum;

	@FindBy(xpath = "//input[@id='id_email' and @name='email']")
	WebElement emailAddInner;

	@FindBy(xpath = "//input[@id='id_password']")
	WebElement passwordInner;

	@FindBy(xpath = "//select[@id='id_gender']")
	WebElement gender;

	public void validateEnrollNowInnerPage() {
		pause(2000);
		clickElement(login);
		pause(2000);
		inputTextThenClickTab(userEmailElement, "ifzal.java2024@gmail.com");
		pause(2000);
		inputText(passwordCSSElement, "Ifzal2024$");
		pause(2000);
		elementEnabled(loginSubmitElement);
		pause(2000);
		clickElement(loginSubmitElement);
		pause(2000);
		pause(2000);
		elementDisplayed(automationElement);
		pause(2000);
		clickElement(automationElement);
		pause(2000);
		switchToChildWindow(driver, mouseHover, enrollNowInner);
		pause(3000);
		validationOfHeader(enrollNowInnerHeader, "Select your course from the dropdown");
		validationOfSubHeader(enrollNowInnerSubHeader, "Please enter your personal and contact information.");
		validationOfOtherHeader(enrollNowInnerOtherHeader, "All fields are required unless marked (optional).");
		verifyTitle(driver, "Enthrall IT - Dashboard");
		verifyCurrentUrl(driver, "https://enthrallit.com/course/dashboard/enrolls/");
	}

	// drop down
	// Select Profession
	// selectByVisibleText()
	public void use_of_dropdown_selectByVisibleText_method() {
		select = new Select(selectProfession);
		select.selectByVisibleText("a Student");
		pause(3000);

	}

	// selectByIndex()
	public void use_of_dropdown_selectByIndex_method() {
		select = new Select(selectCourse);
		select.selectByIndex(2);
		pause(3000);

	}

	// selectByValue()
	public void use_of_dropdown_selectByValue_method() {
		select = new Select(gender);
		select.selectByValue("M");
		pause(3000);
	}

	public void enrollNowInnerFillForm() {
		validateEnrollNowInnerPage();
		pause(2000);
		inputTextThenClickTab(firstname, "John"); // Use of Tab Key
		pause(2000);
		inputTextThenClickTab(middName, "Alexander");
		pause(2000);
		inputTextThenClickTab(lastName, "Smith");
		pause(2000);
		elementSelected(selectProfession);
		pause(2000);
		selectElelementFromDropdownOnebyOne(selectProfession, selectProfessionList);
		pause(2000);
		selectDropdown(selectProfession, "a Student");
		pause(2000);
		elementSelected(selectCourse);
		pause(2000);
		selectDropdown(selectCourse, "Python");
		pause(2000);
		inputTextThenClickTab(phoneNum, "9894556458");
		pause(2000);
		inputTextThenClickTab(emailAddInner, "johnsmith@gmail.com");
		pause(2000);
		inputTextThenClickTab(passwordInner, "Passwordd1$");
		pause(2000);
		selectDropdown(gender, "Male");
		pause(2000);

	}

}