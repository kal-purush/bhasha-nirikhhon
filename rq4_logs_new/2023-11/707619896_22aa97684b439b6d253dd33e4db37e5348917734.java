package homeworks.homework25;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AvailabilityOfElementsOnTheSite extends WebDriverInit {

    final String EXPECTED_TITTLE_PRIVACY_POLICY = "Угода користувача";
    final String EXPECTED_TITTLE_PERSONAL_ACCOUNT = "ОСОБИСТИЙ КАБІНЕТ";

    @Test
    public void checkingPrivacyPolicyLink() {
        openMainPage();
        openToRegistrationBlock();
        checkingPrivacyPolicyLinkInRegistration();
    }

    @Test
    public void checkingPrivacyPolicyTitle(){
        openLegalTermsPage();
        checkingPrivacyPolicyPageTitle();
    }

    @Test
    public void checkingPersonalAccountTitle() {
        openLegalTermsPage();
        checkingPersonalAccountTitleOnLegalTermsPage();
    }
    public void openMainPage() {
        driver.get("https://rozetka.com.ua/");
    }

    public void openToRegistrationBlock() {
                WebElement loginBtn = driver
                .findElement(By.xpath("(//button[contains(@class, 'header__button')])[2]"));
        loginBtn.click();
        WebElement registrationBtn = driver
                .findElement(By.xpath("//button[contains(@class, 'auth-modal__register-link')]"));
        registrationBtn.click();
    }

    public void checkingPrivacyPolicyLinkInRegistration() {
        WebElement privatePolicyDocBtn = driver
                .findElement(By.xpath("(//p[@class='form__caption'])[2]/a[2]"));
        Assert.assertTrue(privatePolicyDocBtn.isDisplayed(), "Privacy policy link is absent");
    }

    public void openLegalTermsPage() {
        driver.get("https://rozetka.com.ua/ua/pages/legal_terms/");
    }

    public void checkingPrivacyPolicyPageTitle() {
        WebElement privacyPolicyPageTitle = driver.findElement(By.xpath("//div[@class='rz-text']//h1"));
        String title = privacyPolicyPageTitle.getText().trim();
        Assert.assertEquals(title, EXPECTED_TITTLE_PRIVACY_POLICY, "Title doesn't contains searching data");
    }

    public void checkingPersonalAccountTitleOnLegalTermsPage() {
        WebElement privacyPolicyPageTitle = driver.findElement(By.xpath("(//div[@class='rz-text']//h3)[6]"));
        Assert.assertTrue(privacyPolicyPageTitle.isDisplayed(), "Title isn't displayed on the page");
        String title = privacyPolicyPageTitle.getText().trim();
        Assert.assertEquals(title, EXPECTED_TITTLE_PERSONAL_ACCOUNT, "Title doesn't contains searching data");
    }
}