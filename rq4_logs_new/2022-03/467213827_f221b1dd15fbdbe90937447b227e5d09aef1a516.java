package md.tekwill.topic1;

public class TestAngajat {

    public static void main(String[] args) {
        Angajat vasilicaTopor007;
        vasilicaTopor007 = new Angajat ("Vasile", 99);

        vasilicaTopor007.nume = "Vasile Topor";
        vasilicaTopor007.varsta = 99;
        vasilicaTopor007.seteazaSalariul(20);
        vasilicaTopor007.seteazaGen("masculin");

        Angajat ionCreanga008;
        ionCreanga008 = new Angajat("Ion", 100);

        ionCreanga008.nume = "Ion Creanga";
        ionCreanga008.varsta = 100;
        ionCreanga008.seteazaSalariul(5000);
        ionCreanga008.seteazaGen("masculin");
    }



}