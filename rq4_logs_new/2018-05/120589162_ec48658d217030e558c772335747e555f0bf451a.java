package SaPrepSonstiges;

import java.util.ArrayList;
import java.util.List;

public class Taube extends Tier {

    public String publicText = "publicTextvonKind";
    private String privateText = "privateText";

    public void taubenMethode() {
        System.out.println("Ich bin ein Taube!");
    }

    @Override
    public void tierMethode() {
        System.out.println("ich bin nicht nur ein Tier, sondern noch mehr...");
    }

    public static void main(String[] args) {

        //Listen erstellen
        List<Tier> tierList = new ArrayList<>();
        List<Taube> taubeList = new ArrayList<>();

        //Objekte erstellen normal und polymorph
        Tier tier1 = new Tier();
        Tier tier2 = new Taube();
        Taube tier3 = new Taube();


        tierList.add(tier1);
        tierList.add(tier2);
        tierList.add(tier3); //Kindklasse wird automatisch auf Elternklasse gecastet


        taubeList.add(((Taube) tier2));//Elternklasse muss von Hand auf Kindklasse gecastet werden


        //Datenfeld Elternklasse
        System.out.println(tier2.publicText);
        //Datenfeld Kindklasse
        System.out.println(((Taube) tier2).publicText);

        //Datenfeld Elternklasse
        System.out.println(((Tier) tier3).publicText);
        //Datenfeld Kindklasse
        System.out.println(tier3.publicText);

        //Vererbung von Methoden
        tier3.taubenMethode();//besitzt Methode direkt
        tier3.tierMethode();//wird geerbt
        tier2.tierMethode();//nutzt direkt Methode der Kindklasse (wegen Konstrukturaufruf new Taube()
        ((Taube) tier2).tierMethode();//Verhält sich als Tier und muss daher für Zugriff gecastet werden
        System.out.println("#####");
        ((Tier) tier2).tierMethode();//Es wird immer die Überschriebene Implemeniterung der Methode verwendet
        System.out.println("#####");
        tier1.tierMethode();

        System.out.println("ForEach von Tierliste:");
        for (Tier tier : tierList) {
            tier.tierMethode();
        }



        //MyParentClass elternKindklasse = new MyParentChildClass();

        //Datenfeld Kindklasse
        //elternKindklasse.
        //Datenfeld Elternklasse
        //System.out.println(((MyParentClass) kindklasse).publicText);


    }
}