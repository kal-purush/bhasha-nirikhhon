package StepDefinitions;

import Pages.DialogContent_Asli;
import Pages.LeftNav_Asli;
import Pages.ParentPage;
import Utilities.GWD;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.support.ui.Select;
import org.testng.Assert;

public class US_708_StepsD extends ParentPage {

    LeftNav_Asli ln=new LeftNav_Asli();
    DialogContent_Asli dc=new DialogContent_Asli();


    @When("Magento sitesine giris")
    public void magentoSitesineGiris() {

        GWD.getDriver().get("https://magento.softwaretestingboard.com/");


    }
    @And("Sayfaya giris yapma")
    public void sayfayaGirisYapma() {

        dc.myClick(dc.sign);
        dc.mySendKeys(dc.email,"ff@gmail.com");
        dc.mySendKeys(dc.password,"Fdeneme1234");


    }

    @Then("Urunun sayfasına giris")
    public void urununSayfasınaGiris() {


        dc.myClick(dc.saleButton);
        dc.myClick(dc.ceket);
        dc.myClick(dc.favUrun1);


    }

    @And("Favorilere Ekle butonuna tiklanir")
    public void favorilereEkleButonunaTiklanir() {


        dc.myClick(dc.wishList1);


    }

    @And("Dogrulama mesajı alınır")
    public void dogrulamaMesajıAlınır() {

        dc.verifyContainsText(dc.favDogrulamaMsg,"Proteus Fitness Jackshirt has been added to your Wish List.Click here to continue shopping.");


    }

    @And("Hesabım sayfasına gidis")
    public void hesabımSayfasınaGidis() {

        Select dropDn=new Select(dc.dropD);
        dropDn.deselectByValue("My Account");
        dc.myClick(dc.hesabim);

    }

    @And("Favorilerim linkine tıklanir")
    public void favorilerimLinkineTıklanir() {

        dc.myClick(dc.favorilerim);
    }

    @Then("Urun eklenmesi dogrul")
    public void urunEklenmesiDogrul() {

        dc.verifyContainsText(dc.fav2,"Item");

    }

    @And("Kullanici urunun uzerine gittiginde delete butonunu cikar")
    public void kullaniciUrununUzerineGittigindeDeleteButonunuCikar() {

        Assert.assertTrue(dc.silBtn.isDisplayed(),"Aranılan sonuç bulunamadı");

    }

    @When("Delete butonuna tıklanır")
    public void deleteButonunaTıklanır() throws InterruptedException {

        dc.myClick(dc.silBtn);
        GWD.getDriver().manage().wait(5);
    }

    @Then("Yeni dogrulama mesaji alinir")
    public void yeniDogrulamaMesajiAlinir() {

        dc.verifyContainsText(dc.silindiMsg,"remowed");


    }


}