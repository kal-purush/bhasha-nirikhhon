package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import utilities.Driver;

public class US01 {

    public US01() {
        PageFactory.initElements(Driver.getDriver(), this);
    }

    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][contains(text(),'Hakkımızda')]")
    public WebElement hakkimizdaButon;

    @FindBy(xpath = "//*[@class='corporate-pages__wrapper mt-lg-5']")
    public WebElement hakkimizdaText;

    @FindBy(xpath = "(//*[.='Hizmetlerimiz'])[1]")
    public WebElement hizmetlerimizButon;
    @FindBy(xpath = "(//*[.='Hizmetlerimiz'])[7]")
    public WebElement hizmetlerimizText;
    @FindBy(xpath = "//*[@class='popup-close']")
    public WebElement popupCloseHome;

    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Kariyer']")
    public WebElement kariyerButon;

    @FindBy(xpath = "//h1[@class='career-pages-hero__title']")
    public WebElement kariyerText;

    @FindBy(xpath = "(//a[.='Sürdürülebilirlik'])[1]")
    public WebElement surdurebilirlikButon;
    @FindBy(xpath = "//h1")
    public WebElement surdurebilirlikText;
    @FindBy(xpath = "(//*[.='İş Ortağımız Olun'])[1]")
    public WebElement isOrtagimizOlunButon;
    @FindBy(xpath = "(//*[@class='menu-item ng-tns-c588362358-0 ng-star-inserted'])[5]")
    public WebElement isOrtagimizOlunText;
    @FindBy(xpath = "(//*[.='Müşteri İlişkileri'])[1]")
    public WebElement musteriIliskileriButon;
    @FindBy(xpath = "(//h1[contains(text(),'Size nasil yardimci olabiliriz?')]")
    public WebElement musteriIliskileriText;
    //                    (//a[@href='/aras-kargo-spor-kulubu'])[1]
    @FindBy(xpath = "(//*[.='Aras Kargo Spor Kulübü'])[1]")
    public WebElement arasKargaSporKulbuButon;
    @FindBy(xpath = "(//*[@href='/genel-mudurluk'])[1]")
    public WebElement bizeUlasinButon;


    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Vizyon, Misyon ve Politikalar']")
    public WebElement vizyonMisyonPolitikalarDrapdownButon;
    @FindBy(partialLinkText = "Vizyon, Misyon ve Politikalar")
    public WebElement vizyonMisyonPolitikalarTextDrapdownButon;


    @FindBy(xpath = "(//a[@href='/hakkimizda/yonetim-ekibimiz'])[1]")
    public WebElement yonetimEkibimizDrapdownButon;
    @FindBy(xpath = "(//*[@href='/hakkimizda/haberler-ve-duyurular'])[1]")
    public WebElement haberDuyurularDrapdownButon;
    @FindBy(xpath = "(//a[@href='/hakkimizda/reklamlarimiz'])[1]")
    public WebElement reklamlarimizDrapdownButon;

    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Bireysel Hizmetlerimiz']")
    public WebElement bireyselHizmetlerimizDrapdownButon;
    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Kurumsal Hizmetlerimiz']")
    public WebElement kurumsalHizmetlerimizDrapdownButon;
    @FindBy(xpath = "(//a[@href='/hizmetlerimiz/ambalaj-urunlerimiz'])[1]")
    public WebElement ambalajUrunlerimizDrapdownButon;


    @FindBy(xpath = "(//a[@href='/kariyer/araslar-bizde-onem-tasir'])[1]")
    public WebElement araslarBizdeOnemTasirDrapdownButon;
    @FindBy(xpath = "(//*[@href='/kariyer/gelecegin-bizde-onem-tasir'])[1]")
    public WebElement geleceginBizdeOnemTasirDrapdownButon;
    @FindBy(xpath = "(//*[@href='/kariyer/iyi-bir-dunya-bizde-onem-tasir'])[1]")
    public WebElement iyiBirDunyaBizdeOnemTasirDrapdownButon;
    @FindBy(xpath = "(//*[@href='/kariyer/teknoloji-bizde-onem-tasir'])[1]")
    public WebElement teknolojiBizdeOnemTasirDrapdownButon;


    @FindBy(xpath = "(//*[@href='/surdurulebilirlik/surdurulebilirlik-rotasi'])[1]")
    public WebElement surdurulebilirlikRotasiDrapdownButon;
    @FindBy(xpath = "(//*[@href='/surdurulebilirlik/sosyal-sorumluluk-platformlarimiz'])[1]")
    public WebElement sosyalSorumlulukPlatformlarimizDrapdownButon;
    @FindBy(xpath = "(//*[@href='/surdurulebilirlik/kalite-yonetim-sistemi'])[1]")
    public WebElement kaliteYonetimSistemiDrapdownButon;


    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Acentemiz Olun']")
    public WebElement acentemizOlunDrapdownButon;
    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][contains(text(),'Aras Burası Noktası Olun')]")
    public WebElement arasBurasiNoktasiOlunDrapdownButon;
    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][normalize-space()='Esnaf Kurye Olun']")
    public WebElement esnafKuryeOlunDrapdownButon;
    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][contains(text(),'Aracınızı Kiralayın')]")
    public WebElement araciniziKiralayinDrapdownButon;
    @FindBy(xpath = "//a[@class='ng-tns-c588362358-0'][contains(text(),'Sık Sorulan Sorular')]")
    public WebElement sikSorulanSorularDrapdownButon;


    @FindBy(xpath = "(//*[@href='/musteri-iliskileri'])[2]")
    public WebElement arasaSorDrapdownButon;
    @FindBy(xpath = "(//*[@href='/musteri-iliskileri'])[3]")
    public WebElement cagriMerkeziDrapdownButon;
    @FindBy(xpath = "(//*[@href='/musteri-iliskileri/hasar-tazmin'])[1]")
    public WebElement hasarTazminDrapdownButon;
    @FindBy(xpath = "(//*[@href='/musteri-iliskileri/geri-bildirim-formu'])[1]")
    public WebElement geriBildirimFormuDrapdownButon;


    @FindBy(xpath = "(//*[@href='/musteri-iliskileri/sss'])[1]")
    public WebElement ssSDrapdownButon;


    @FindBy(xpath = "(//*[@href='/aras-kargo-spor-kulubu'])[1]")
    public WebElement arasKargoSporKulubuDrapdownButon;


    @FindBy(xpath = "(//*[@href='/iletisim'])[1]")
    public WebElement iletisimDrapdownButon;
    @FindBy(xpath = "(//*[@href='/genel-mudurluk'])[2]")
    public WebElement genelMudurlukDrapdownButon;
    @FindBy(xpath = "(//*[@href='/bolge-mudurluklerimiz'])[1]")
    public WebElement bolgeMudurluklerimizDrapdownButon;
    @FindBy(xpath = "(//*[@href='/subelerimiz'])[1]")
    public WebElement subelerimizDrapdownButon;
    @FindBy(xpath = "(//*[@href='/aras-burasi-noktalarimiz'])[1]")
    public WebElement arasBurasiNoktalarimizDrapdownButon;
    @FindBy(xpath = "(//*[@href='/musterimiz-olun'])[1]")
    public WebElement musterimizOlunDrapdownButon;
}