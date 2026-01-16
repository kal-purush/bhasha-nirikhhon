import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import pageObjects.ContactUsFormPage;

public class FillFirstNameTest {

    static private WebDriver driver = null;
    private ContactUsFormPage contactUsFormPage;

    private String firstName;

    @BeforeTest
    public void openBrowser() {
        driver = new ChromeDriver();
        driver.get("http://www.andrijk.ixloo.com/contactus");
    }

    @Test
    public void fillContactUs() {
        firstName = "Diana";
        contactUsFormPage = new ContactUsFormPage(driver);
        contactUsFormPage.fillFirstNameField(firstName);
        Assert.assertEquals(contactUsFormPage.getFirstName(), firstName);
    }

    @AfterTest
    public void closeBrowser() {
        if (driver != null)
            driver.quit();
    }
}