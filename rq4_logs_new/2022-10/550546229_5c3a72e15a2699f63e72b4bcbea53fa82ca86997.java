import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class ChangeUsernameTests extends  BaseTest {


    //    Create a test to change user’s name and verify it’s changed
//    (hint: make sure to always generate a new user’s name)

    @Test
    public void changeProfileNameTest () throws InterruptedException {
        String newName  = getRandomString();
        String currentPassword = "te$t$tudent";

        //login
        provideEmail("demo@class.com");
        providePassword();
        clickSubmitBtn();

        //profile page
        openUserProfilePage();
        setName(newName);
        setCurrentPassword(currentPassword);
        Thread.sleep(2000);
        Assert.assertTrue(getSuccessPopUp().isDisplayed());
        Assert.assertEquals(newName, getUsersNameOnHeader());
        Assert.assertEquals(newName, getUsernameProfileField());
    }

    public String getRandomString() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private String getUsernameProfileField() {
        return driver.findElement(By.cssSelector("#inputProfileName")).getAttribute("_value");
    }

    private String getUsersNameOnHeader() {
        return driver.findElement(By.cssSelector("#userBadge span.name")).getText();
    }

    private WebElement getSuccessPopUp() {
        return driver.findElement(By.cssSelector(".success.show"));
    }

    private void setCurrentPassword(String currentPassword) {
        WebElement bb = driver.findElement(By.cssSelector("#inputProfileCurrentPassword"));
        bb.click();
        bb.sendKeys("te$t$tudent");
        driver.findElement(By.cssSelector(".btn-submit")).click();
    }

    private void setName(String newName) {
        WebElement tt = driver.findElement(By.cssSelector("#inputProfileName"));
        tt.click();
        tt.clear();
        tt.sendKeys(newName);
    }

    private void openUserProfilePage() {
        WebElement profileIcon = driver.findElement(By.cssSelector("#userBadge span.name"));
        profileIcon.click();
    }


}