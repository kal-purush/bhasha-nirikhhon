package com.tests.withPages.salesforce;

import com.tests.withPages.salesforce.pages.SFCommon;
import com.tests.withPages.salesforce.pages.SFHomePage;
import com.tests.withPages.salesforce.pages.SFNewContactPage;
import com.usf.utils.DataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import test.BaseUITest;

import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;


public class SalesForceDemoTestFAIL extends BaseUITest {

    private final Logger log = LoggerFactory.getLogger(SalesForceDemoTestFAIL.class);

    @DataProvider(name = "Authentication")
    private Object[][] getCredentials() {
        return new Object[][]{{
                DataConverter.getMetadataValue("MD_Salesforce_Credentials_SalesUser"),
                DataConverter.getMetadataValue("MD_Salesforce_Credentials_SalesPassword")
        }};
    }

    @Test(dataProvider = "Authentication", description = "This test is used for demonstration of the Selenide framework.")
    public void salesForceDemoTest_2(String name, String password) {

        SFHomePage homePage = open("https://www.salesforce.com/", SFHomePage.class);

        homePage.moveToLoginPage()
                .loginWithUserNameAndPassword(name, password);

        SFCommon commonPage = homePage.moveToCommonPage();

        commonPage
                .clickContactsLink()
                .clickNewContactButton();

        SFNewContactPage newContactPage = homePage.moveToNewContactPage();

        newContactPage
                .createNewContact(
                        "Joe",
                        "AutoTest1",
                        "555-555-5555",
                        "joe@mailinator.com",
                        "123 Test Dr",
                        "Chicago",
                        "IL",
                        "60601",
                        DataConverter.getBirthdayValue("<AUTO_DOB|35_45"))
                .saveContact();

        commonPage.globalSearch(DataConverter.getKeywordValue("<FullName>"));

        /* TestNg Verification*/
        $(newContactPage.getNewContactName()).shouldHave(text("Joe AutoTest2"));
    }
}