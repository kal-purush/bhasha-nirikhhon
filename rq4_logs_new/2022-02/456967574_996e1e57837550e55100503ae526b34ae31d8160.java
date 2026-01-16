package practicum4B;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AutoHuurTest {
    @Test
    public void totaalPrijsNormaal() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(5);
        assertEquals(25.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenHuurder() {
        Auto a = new Auto("testauto", 10);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setAantalDagen(5);
        assertEquals(50.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAuto() {
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(5);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoEnHuurder() {
        AutoHuur ah = new AutoHuur();
        ah.setAantalDagen(5);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsNormaalNulDagen() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsNormaalNegatiefDagen() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenHuurderNulDagen() {
        Auto a = new Auto("testauto", 10);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenHuurderNegatiefDagen() {
        Auto a = new Auto("testauto", 10);
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoNulDagen() {
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoNegatiefDagen() {
        Klant k = new Klant("testklant");
        k.setKorting(50.0);
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoGeenHuurderNulDagen() {
        AutoHuur ah = new AutoHuur();
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoGeenHuurderNegatiefDagen() {
        AutoHuur ah = new AutoHuur();
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsNormaalGeenKorting() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(5);
        assertEquals(50.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoGeenKorting() {
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(5);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsNulDagenGeenKorting() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsNegatiefDagenGeenKorting() {
        Auto a = new Auto("testauto", 10);
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setGehuurdeAuto(a);
        ah.setHuurder(k);
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoNulDagenGeenKorting() {
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(0);
        assertEquals(0.0, ah.totaalPrijs());
    }

    @Test
    public void totaalPrijsGeenAutoNegatiefDagenGeenKorting() {
        Klant k = new Klant("testklant");
        AutoHuur ah = new AutoHuur();
        ah.setHuurder(k);
        ah.setAantalDagen(-1);
        assertEquals(0.0, ah.totaalPrijs());
    }
}