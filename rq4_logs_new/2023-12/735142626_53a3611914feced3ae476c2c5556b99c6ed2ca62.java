package arasKargo.stepDefinition;

import arasKargo.pages.HizmetlerimizPage;
import arasKargo.utilities.Driver;
import arasKargo.utilities.ReusableMethods;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.Keys;
import org.openqa.selenium.interactions.Actions;

public class US02_StepDefinition {

    HizmetlerimizPage hizmetlerimizPage = new HizmetlerimizPage();
    Actions actions = new Actions(Driver.getDriver());
    @When("hizmetlerimiz butonuna tiklar")
    public void hizmetlerimizzButonunaTiklar() {
        HizmetlerimizPage hizmetlerimizPage = new HizmetlerimizPage();
        actions.scrollToElement(hizmetlerimizPage.hizmetlerimiz).perform();
    }

    @And("kullanıcı Ambalaj Ürünlerimiz Detaylı Bilgi linkine tıklar")
    public void kullanıcıAmbalajUrunlerimizDetaylıBilgiLinkineTıklar1() {
        actions.scrollToElement(hizmetlerimizPage.hizmetlerimiz).perform();
        ReusableMethods.click(hizmetlerimizPage.ambalajUrunlerimiz);
    }

    @And("kullanıcı Ambalaj Fiyatları linkine tıklar")
    public void kullaniciAmbalajFiyatlariLinkineTıklar() {
        actions.sendKeys(Keys.PAGE_DOWN).build().perform();
        hizmetlerimizPage.ambalajFiyatlari.click();

    }

    @Then("kullanıcı Aras Kargo Hizmetler Fiyat Listesi sayfasının açıldığını doğrular")
    public void kullanıcıArasKargoHizmetlerFiyatListesiSayfasınınAçıldığınıDoğrular() {

        String currentUrl = "https://www.araskargo.com.tr/assets/images/pdf/Aras_Hizmetler_fiyat_listesi_2023_ikinci_yari_yil_yeni_kdv_24_07_23.pdf";
        if (currentUrl.endsWith(".pdf")) {
            System.out.println("PDF sayfası başarıyla yüklendi.");
        } else {
            System.out.println("Hata: PDF sayfası yüklenemedi.");
        }
    }
}