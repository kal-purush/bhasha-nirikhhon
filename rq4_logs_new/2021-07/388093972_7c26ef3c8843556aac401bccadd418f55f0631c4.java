package login;

import libs.ExcelDriver;
import org.junit.Test;
import parenTest.ParentTest;

import java.io.IOException;
import java.util.Map;

public class LogInNewWithDataFromExcel extends ParentTest {
    @Test
    public void validLogin() throws IOException {
        ExcelDriver excelDriver = new ExcelDriver();
        Map dataForValidLogin = excelDriver.getData(configProperties.DATA_FILE(),"validLogOn");

        loginPage.openLoginPage();
        loginPage.enterLogin(dataForValidLogin.get("login").toString());
        loginPage.enterPassWord(dataForValidLogin.get("pass").toString());
        loginPage.clickButtonVhod();

        checkExpectedResult("Avatar is not present", homePage.isAvatarDisplayed());
//        Assert.assertTrue("Avatar is not present", homePage.isAvatarDisplayed());  // valid
    }

}