package wzorce.facade;

public class ApplicationFasada {

    private DrzwiMieszkanie1 drzwiMieszkanie1 = new DrzwiMieszkanie1();
    private DrzwiMieszkanie2 drzwiMieszkanie2 = new DrzwiMieszkanie2();

    private KlimatyzacjaMieszkania klimatyzacjaMieszkania = new KlimatyzacjaMieszkania();
    private KlimatyzacjaNaKorytarzu klimatyzacjaNaKorytarzu = new KlimatyzacjaNaKorytarzu();

    private SwiatlaWMieszkaniach swiatlaWMieszkaniach = new SwiatlaWMieszkaniach();
    private SwiatlaNaKorytarzu swiatlaNaKorytarzu = new SwiatlaNaKorytarzu();

    public void otworzMieszkania(){
        drzwiMieszkanie1.otworzMieszkanie();
        drzwiMieszkanie2.otworzMieszkanie();
    }

    public void zamknijMieszkania(){
        drzwiMieszkanie1.zamknijMieszkanie();
        drzwiMieszkanie2.zamknijMieszkanie();
    }

    public void zamknijDrzwi(){
        drzwiMieszkanie1.zamknijDrzwi();
        drzwiMieszkanie2.zamknijDrzwi();
    }

    public void otworzDrzwi(){
        drzwiMieszkanie1.otworzDrzwi();
        drzwiMieszkanie2.otworzDrzwi();
    }

    public void wlaczKlimatyzacje(){
        klimatyzacjaMieszkania.wlaczKlimatyzajce();
        klimatyzacjaNaKorytarzu.wlaczKlimatyzajce();
    }

    public void wylaczKlimatyzacje(){
        klimatyzacjaMieszkania.wylaczKlimatyzacje();
        klimatyzacjaNaKorytarzu.wylaczKlimatyzacje();
    }

    public void zapalSwiatla(){
        swiatlaWMieszkaniach.zapalSwiatla();
        swiatlaNaKorytarzu.zapalSwiatla();
    }

    public void zgasSwiatla(){
        swiatlaNaKorytarzu.zgasSwiatla();
        swiatlaWMieszkaniach.zgasSwiatla();
    }

}