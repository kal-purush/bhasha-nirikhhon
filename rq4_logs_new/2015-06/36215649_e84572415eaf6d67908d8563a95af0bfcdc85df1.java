package Tests.I.UA;

import helperIua.ComposeMailHelper;
import helperIua.LoginHelper;
import org.testng.annotations.BeforeClass;
import pageObjectsIua.Drafts;
import pageObjectsIua.MailBoxPage;
import pageObjectsIua.SentMailPage;

/**
 * Created by Liliia_Klymenko on 02-Jun-15.
 */
public class BaseTest {


    protected static LoginHelper loginHelper;
    protected static ComposeMailHelper composeMailHelper;
    protected static MailBoxPage mailBoxPage;
    protected static SentMailPage sentMailPage;
    protected static Drafts drafts;



    @BeforeClass
    public void initPages() {
        loginHelper = new LoginHelper();
        composeMailHelper = new ComposeMailHelper();
        mailBoxPage = MailBoxPage.getMailBoxPage();
        sentMailPage = SentMailPage.getSentMailPage();
        drafts = Drafts.getDraftsPage();
    }

/*    @AfterClass
    public void startWebDriver() {
        Page.quitDriver();
    }*/
}