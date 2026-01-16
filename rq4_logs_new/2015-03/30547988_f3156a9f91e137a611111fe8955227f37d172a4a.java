package functional;

import entities.Advertisement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import pages.AddNewAdvertPage;
import pages.HomePage;
import pages.PreviewPage;

public class AddAdvertTest extends AbstractTest {

    Logger logger = LoggerFactory.getLogger(AddAdvertTest.class);

    @Test
    public void addAdvertTest() {
        logger.info("AddAdvertTest STARTED");
        openAddNewAdvertPage();
        addNewAdvert();
        PreviewPage previewPage = new PreviewPage(browser);
        Assert.assertTrue(previewPage.previewIsOpened());
        logger.info("AddAdvertTest ENDED");
    }

    public void openAddNewAdvertPage() {
        HomePage homePage = new HomePage(browser);
        homePage.open();
        homePage.clickAddNewAdvert();
    }

    public void addNewAdvert(){
        Advertisement adv = new Advertisement(true);
        AddNewAdvertPage addNewAdvertPage = new AddNewAdvertPage(browser);
        addNewAdvertPage.setAdv(adv);
        addNewAdvertPage.openPreview();
    }


}