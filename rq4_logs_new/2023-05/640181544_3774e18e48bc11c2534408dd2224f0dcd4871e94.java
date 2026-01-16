package Pages;

import Utilities.BasicDriver;
import Utilities.MyMethods;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.util.List;

public class Yuksel_POM extends MyMethods {
    public Yuksel_POM() {PageFactory.initElements(BasicDriver.getDriver(),this);}
    @FindBy(xpath = "(//span[text()='Entrance Exams'])[1]")
    private WebElement entranceExam1;

    @FindBy(xpath = "(//span[text()='Entrance Exams'])[2]")
    private WebElement entranceExam2;

    @FindBy(xpath = "(//span[text()='Setup'])[2]")
    private WebElement setup;

    @FindBy(xpath = "//input[@data-placeholder='Name']")
    private WebElement nameField;

    @FindBy(css = "svg[class='svg-inline--fa fa-plus']")
    private WebElement addButton;

    @FindBy(xpath = "//span[text()='Academic Period']")
    private List<WebElement> academicPeriod;

    @FindBy(xpath = "//span[text()='Grade Level']")
    private List<WebElement> gradeLevel;

    @FindBy(xpath = "//span[text()=' Staj 2023 ']")
    private WebElement firstAcademicPeriod;

    @FindBy(xpath = "//span[text()='Save']")
    private WebElement saveButton;

    @FindBy(xpath = "//span[text()=' 11 ']")
    private WebElement gradeLevelFirst;

    @FindBy(xpath = "//div[contains(text(),'successfully')]")
    private WebElement successMessage;

    public WebElement getSuccessMessage() {
        return successMessage;
    }

    public WebElement getGradeLevelFirst() {
        return gradeLevelFirst;
    }

    public WebElement getFirstAcademicPeriod() {
        return firstAcademicPeriod;
    }

    public WebElement getSaveButton() {
        return saveButton;
    }

    public WebElement getEntranceExam1() {
        return entranceExam1;
    }

    public WebElement getEntranceExam2() {
        return entranceExam2;
    }

    public WebElement getSetup() {
        return setup;
    }

    public WebElement getNameField() {
        return nameField;
    }

    public WebElement getAddButton() {
        return addButton;
    }

    public List<WebElement> getAcademicPeriod() {
        return academicPeriod;
    }

    public List<WebElement> getGradeLevel() {
        return gradeLevel;
    }
}

