import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class LanguagePageTests extends BaseTest {
    @BeforeTest
    public void beforeTest() {
        languagePage.openPage();
    }

    @AfterTest
    public void afterTest() {
        driver.quit();
    }

    @Test
    public void verifyDefaultLanguage() {
        //homePage.clickOnLanquageChange();
        Assert.assertTrue(languagePage.isLanguageChecked(Language.EN), "Language is not selected");
        Assert.assertEquals(languagePage.getLanguageValue(Language.EN), "English - EN");
    }

    @Test
    public void verifyDefaultCurrency() {
        //homePage.clickOnLanquageChange();
        Assert.assertEquals(languagePage.getDefaultCurrency(), "$ - USD - US Dollar (Default)");
    }

    @Test
    public void verifyCurrencyChange() {
        homePage.clickOnLanquageChange();
        languagePage.changeCurrency(String.valueOf(Currency.PLN));
        Assert.assertEquals(languagePage.getDefaultCurrency(), "PLN - Polish Zloty");
    }
}