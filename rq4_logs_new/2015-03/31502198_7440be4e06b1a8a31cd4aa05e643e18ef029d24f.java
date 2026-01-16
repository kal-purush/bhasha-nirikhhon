package functional;

import entities.Advertisement;
import org.testng.Assert;
import pages.AdvertPage;
import pages.HomePage;

/**
 * Created by c267 on 11.03.2015.
 */
public class PositiveAdvertisementTest extends AbstractTest {

   // @Test
    public void newAdvertTest() {
        openAddAdvertisement();
    }


    public  void  openAddAdvertisement()
    {
        HomePage homePage = new HomePage(browser);
        homePage.open();
        AdvertPage advertpage = new AdvertPage(browser);
        advertpage.newAdvert();
        advertpage.isOpened();
    }

    public void addNewAdvert(boolean correct) {
        Advertisement advertisement;
        if (correct)
            advertisement = new Advertisement(true);
        else
            advertisement = new Advertisement(false);

        AdvertPage advertpage = new AdvertPage(browser);
        advertpage.fillAdvert(advertisement);
        Assert.assertTrue(advertpage.isPreviewOpened());
    }
}
