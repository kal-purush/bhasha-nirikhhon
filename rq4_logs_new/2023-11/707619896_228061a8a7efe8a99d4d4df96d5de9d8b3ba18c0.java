package homeworks.homework25;

import init.WebDriverInit;
import org.openqa.selenium.By;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.Thread.sleep;

public class CheckingElementsOnTheSite_v2 extends WebDriverInit {

    final String EXPECTED_TITTLE_PRIVACY_POLICY = "Угода користувача";
    final String EXPECTED_TITTLE_PERSONAL_ACCOUNT = "ОСОБИСТИЙ КАБІНЕТ";

    @Test
    public void checkingElements() throws InterruptedException {
        openMainPage();
        goToRegistrationBlock();
        checkingPrivacyPolicyLinkInRegistration();
        changeCFCookie("2eeImjDdy07qf4fLS0jtP13u5u8l3ItEEhCkfWVlYhM-1701020941-0-1-a0523e9a.8c804b95.f753617a-0.2.1701020941");
        checkingPrivacyPolicyPageTitle();
        checkingPersonalAccountTitleOnLegalTermsPage();

    }

    public void openMainPage() {
        driver.get("https://rozetka.com.ua/");
    }

    public void goToRegistrationBlock() throws InterruptedException {
        WebElement loginBtn = driver
                .findElement(By.xpath("(//button[contains(@class, 'header__button')])[2]"));
        loginBtn.click();
        WebElement registrationBtn = driver
                .findElement(By.xpath("//button[contains(@class, 'auth-modal__register-link')]"));
        registrationBtn.click();
    }

    public void checkingPrivacyPolicyLinkInRegistration() throws InterruptedException {
        WebElement privatePolicyDocBtn = driver
                .findElement(By.xpath("(//p[@class='form__caption'])[2]/a[2]"));
        Assert.assertTrue(privatePolicyDocBtn.isDisplayed(), "Privacy policy link is absent");
        privatePolicyDocBtn.click();
        Set<String> handles = driver.getWindowHandles();
        List<String> listHandles = new ArrayList<>(handles);
        driver.switchTo().window(listHandles.get(1));
    }

    public void changeCFCookie(String value) throws InterruptedException {
        Cookie cookie = new Cookie("cf_clearance",value);
        driver.manage().deleteCookieNamed("cf_clearance");
        driver.manage().addCookie(cookie);
        driver.navigate().refresh();
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