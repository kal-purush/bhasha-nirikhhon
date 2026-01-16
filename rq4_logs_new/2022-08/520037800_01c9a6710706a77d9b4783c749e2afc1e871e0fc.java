import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;

public class AutomationExerciseLoginLoginPage {
    WebDriver driver;
    public AutomationExerciseLoginLoginPage(WebDriver driver){

        this.driver = driver;
        PageFactory.initElements(this.driver, this );
    }
    // @FindBy(xpath = "//input[@data-qa=\"login-email\"]")
    // public WebElement LoginEmailField;

    // @FindBy(xpath = "//input[@data-qa=\"login-password\"]")
    // public WebElement LoginPasswordField;

    // @FindBy(xpath = "//button[@data-qa=\"login-button\"]")
    //public WebElement LoginButton;

    @FindBy(xpath = "//h2[contains(text(),'Login to your account')]")
    public WebElement LoginToYourAccount;

    @FindBy(xpath = "//input[@name=\"email\"]")
    public WebElement EmailAddress;

    @FindBy(xpath = "//input[@name=\"password\"]")
    public WebElement Password;

    @FindBy(xpath = "//button[contains(text(),'Login')]")
    public WebElement Login;

    public String getLoginToYourAccount(){

        String LoginToYourAccountText = driver.findElement(By.xpath("//h2[contains(text(),'Login to your account')]")).getText();
        return LoginToYourAccountText;

    }

    public String getIncorrectLoginText(){

        String IncorrectLoginText = driver.findElement(By.xpath("//p[contains(text(),'Your email or password is incorrect!')]")).getText();
        return IncorrectLoginText;

    }

    @FindBy(xpath = "//p[contains(text(),'Your email or password is incorrect!')]")
    public WebElement IncorrectLoginText;


    public void CorrectLoginCredentialsAutomationExercise(){

        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
        wait.until(ExpectedConditions.visibilityOf(LoginToYourAccount));
        String LoginToYourAccount = getLoginToYourAccount();
        Assert.assertEquals(LoginToYourAccount, "Login to your account");

        wait.until(ExpectedConditions.visibilityOf(EmailAddress));
        wait.until(ExpectedConditions.elementToBeClickable(EmailAddress));
        EmailAddress.sendKeys(AutomationExerciseAccountInfo.LoginEmailAddress);


        wait.until(ExpectedConditions.visibilityOf(Password));
        wait.until(ExpectedConditions.elementToBeClickable(Password));
        Password.sendKeys(AutomationExerciseAccountInfo.LoginPassword);

        wait.until(ExpectedConditions.visibilityOf(Login));
        wait.until(ExpectedConditions.elementToBeClickable(Login));
        Login.click();

    }
    public void IncorrectLoginCredentialsAutomationExercise(){

        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
        wait.until(ExpectedConditions.visibilityOf(LoginToYourAccount));
        String LoginToYourAccount = getLoginToYourAccount();
        Assert.assertEquals(LoginToYourAccount, "Login to your account");

        wait.until(ExpectedConditions.visibilityOf(EmailAddress));
        wait.until(ExpectedConditions.elementToBeClickable(EmailAddress));
        EmailAddress.sendKeys(AutomationExerciseAccountInfo.LoginIncorrectEmailAddress);


        wait.until(ExpectedConditions.visibilityOf(Password));
        wait.until(ExpectedConditions.elementToBeClickable(Password));
        Password.sendKeys(AutomationExerciseAccountInfo.LoginIncorrectPassword);

        wait.until(ExpectedConditions.visibilityOf(Login));
        wait.until(ExpectedConditions.elementToBeClickable(Login));
        Login.click();

        wait.until(ExpectedConditions.visibilityOf(IncorrectLoginText));
        wait.until(ExpectedConditions.visibilityOf(IncorrectLoginText));
        String IncorrectLoginText = getIncorrectLoginText();
        Assert.assertEquals(IncorrectLoginText, "Your email or password is incorrect!");

    }
}