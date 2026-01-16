package arasKargo.stepDefinition;

import arasKargo.pages.HomePagePage;
import arasKargo.pages.MusteriHizmetleriPage;
import arasKargo.utilities.Driver;
import arasKargo.utilities.ReusableMethods;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import net.bytebuddy.asm.Advice;
import org.junit.Assert;
import org.openqa.selenium.Keys;
import org.openqa.selenium.interactions.Actions;

import java.security.Key;

import static org.junit.Assert.*;


public class US08_StepDefinition {

    HomePagePage homePagePage = new HomePagePage();
    MusteriHizmetleriPage musteriHizmetleriPage = new MusteriHizmetleriPage();
    Actions actions = new Actions(Driver.getDriver());



    @Then("Kullanici dashboard daki Musteri İliskileri kisminin görüldügünü dogrular")
    public void kullaniciDashboardDakiMusteriİliskileriKismininGörüldügünüDogrular() {
        ReusableMethods.bekle(2);
        assertTrue(musteriHizmetleriPage.musteriHizmetleri.getText().contains("Müşteri İlişkileri"));

    }

    @And("Kullanici dashboard daki Musteri Iliskileri kismina tiklar")
    public void kullaniciDashboardDakiMusteriIliskileriKisminaTiklar() {
        musteriHizmetleriPage.musteriHizmetleri.sendKeys(Keys.ENTER);
        ReusableMethods.bekle(1);
    }

    @And("Kullanici Geri Bildirim Formu  yazisi gozukene kadar sayfayi asagi kaydirir")
    public void kullaniciGeriBildirimFormuYazisiGozukeneKadarSayfayiAsagiKaydirir() {
        actions.scrollToElement(musteriHizmetleriPage.sayfayaGiticinKaydirma).perform();
        ReusableMethods.bekle(1);
    }

    @And("Kullanici Geri Bildirim Formu kisminin altindaki Sayfaya Git yazisina tiklar")
    public void kullaniciGeriBildirimFormuKismininAltindakiSayfayaGitYazisinaTiklar() {
        musteriHizmetleriPage.getGeriBildirimsayfayaGit.sendKeys(Keys.ENTER);
        ReusableMethods.bekle(1);
    }

    @And("Kullanici Bireysel kismina tiklar")
    public void kullaniciBireyselKisminaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.bireyselSekmesi);
    }

    @And("Kullanici <Kategori Secin..> yazisi gozukene kadar sayfayi asagi kaydirir")
    public void kullaniciKategoriSecinYazisiGozukeneKadarSayfayiAsagiKaydirir() {
        ReusableMethods.scroll(musteriHizmetleriPage.kategoriSecin);
        ReusableMethods.bekle(1);
    }

    @And("Kullanici Kategori Secin.. yazisina tiklar")
    public void kullaniciKategoriSecinYazisinaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.kategoriSecin);
        ReusableMethods.bekle(1);
    }

    @Then("Kullanici acilan dropdown daki BILGI butonuna tiklar")
    public void kullaniciAcilanDropdownDakiBILGIButonunaTiklar() {
         ReusableMethods.click(musteriHizmetleriPage.bilgi);
        //Assert.assertTrue(musteriHizmetleriPage.kategoriSecin.getText().contains("BİLGİ"));
         ReusableMethods.bekle(1);
    }

    @Then("Kullanici acilan dropdown daki ISLEM-TALEP butonuna tiklar")
    public void kullaniciAcilanDropdownDakiISLEMTALEPButonunaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.islemTalep);
        ReusableMethods.bekle(1);
    }

    @Then("Kullanici acilan dropdown daki ONERI butonuna tiklar")
    public void kullaniciAcilanDropdownDakiONERIButonunaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.oneri);
        ReusableMethods.bekle(1);
    }

    @Then("Kullanici acilan dropdown daki SIKAYET-PROBLEM butonuna tiklar")
    public void kullaniciAcilanDropdownDakiSIKAYETPROBLEMButonunaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.sikayetProblem);
        ReusableMethods.bekle(1);
    }

    @Then("Kullanici acilan dropdown daki TESEKKURLER butonuna tiklar")
    public void kullaniciAcilanDropdownDakiTESEKKURLERButonunaTiklar() {
        ReusableMethods.click(musteriHizmetleriPage.tesekkurler);
        ReusableMethods.bekle(1);
    }
}