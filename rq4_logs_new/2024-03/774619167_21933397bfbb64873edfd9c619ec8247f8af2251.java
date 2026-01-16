
//200 gram ongezouten roomboter
//200 gram witte bastard suiker
//400 gram zelfrijzend bakmeel
//1 stuk(s) ei
//8 gram vanillesuiker
//1 snuf zout
//1.5 kilo zoetzure appels
//75 gram kristal suiker
//3 theelepels kaneel
//15 gram paneermeel
public class ApplePieRecipe {

    Ingredient boter = new Ingredient(200,"gram ", "ongezouten roomboter");
    Ingredient suiker = new Ingredient(200,"gram", "witte bastard suiker");
    Ingredient bakmeel = new Ingredient(400, "gram", "zelfrijzend bakmeel");
    Ingredient ei = new Ingredient(1, "stuk", "ei");

    Ingredient vanillesuiker = new Ingredient(8, "gram",  "vanillesuiker");
    Ingredient zout = new Ingredient(1, "snuf", "zout");
    Ingredient appels = new Ingredient(1.5, "kilo",  "zoetzure appels");
    Ingredient kristalsuiker = new Ingredient(75, "gram",  "kristal suiker");
Ingredient kaneel = new Ingredient(3, "theelepels", "kaneel" );
Ingredient paneermeel = new Ingredient(15, "gram" , "paneermeel");

    public void voorverwarmen() {
        System.out.println("Verwarm de oven van te voren op 170 graden Celsius (boven en onderwarmte)");
    }
    public void opkloppen(){
        System.out.println("Klop het ei los en verdeel deze in twee delen. De ene helft is voor het deeg, het andere deel is voor het bestrijken van de appeltaart.");
    }
    public void mixen(){
        System.out.println("Meng de boter, bastard suiker, zelfrijzend bakmeel, een helft van het ei, vanille suiker en een snufje zout tot een stevig deeg en verdeel deze in 3 gelijke delen.");
        }
    public void schilMeng(){

        System.out.println("Schil nu de appels en snij deze in plakjes. Vermeng in een kopje de suiker en kaneel.");
    }
    public void invetten(){
        System.out.println("Vet de springvorm in en bestrooi deze met bloem");
    }
    public void bodemdeeg() {
        System.out.println("Gebruik een deel van het deeg om de bodem van de vorm te bedekken. Gebruik een deel van het deeg om de rand van de springvorm te bekleden. Strooi het paneermeel op de bodem van de beklede vorm. De paneermeel neemt het vocht van de appels op.");
    }
    public void appelbereiden() {
        System.out.println("Doe de heft van de appels in de vorm en strooi hier 1/3 van het kaneel-suiker mengsel overheen. Meng de ander helft van de appels met het overgebleven kaneel-suiker mengsel en leg deze in de vorm.");
    }
    public void deegstroken() {
        System.out.println("Rol het laatste deel van de deeg uit tot een dunne lap en snij stroken van ongeveer 1 cm breed.");
    }
    public void bovenkant() {
        System.out.println("Leg de stroken kruislings op de appeltaart. Met wat extra deegstroken werk je de rand rondom af. Gebruik het overgebleven ei om de bovenkant van het deeg te bestrijken");
    }
    public void bakken() {
        System.out.println("Zet de taart iets onder het midden van de oven. Bak de taart in 60 minuten op 170 graden Celsius (boven en onderwarmte) gaar en goudbruin.");
    }
public void stappenPlan() {
    System.out.println("doorlopen de volgende stappen");
    voorverwarmen();
    opkloppen();
    mixen();
    schilMeng();
    invetten();
    bodemdeeg();
    appelbereiden();
    deegstroken();
    bovenkant();
    bakken();
}

public void ingredientenLijst(){
    System.out.println(ei);
    System.out.println(boter);
    System.out.println(suiker );
    System.out.println(bakmeel);
    System.out.println(vanillesuiker);
    System.out.println(zout);
    System.out.println(appels);
    System.out.println(kristalsuiker);
    System.out.println(kaneel);
    System.out.println(paneermeel);


}

    public ApplePieRecipe() {
    }
}