 package hu.petrik;

import java.util.ArrayList;
import java.util.List;

public class Bank {
    private List<Szamla> szamlaLista;


    public Bank(List<Szamla> szamlaLista) {
        this.szamlaLista = szamlaLista;
    }

    public Szamla szamlanyitas(int hitelKeret, Tulajdonos tulajdonos){
        HitelSzamla ht = new HitelSzamla(tulajdonos, hitelKeret);

        return ht;
    }

    public int getOsszegyenleg(Tulajdonos tulajdonos) {
        int osszEgyenleg = 0;
        if (szamlaLista.contains(tulajdonos)){
            for (int i = 0; i < szamlaLista.size(); i++) {
                osszEgyenleg += szamlaLista.get(i).aktualisEgyenleg;
            }
        }
        return osszEgyenleg;
    }

    public Szamla LegnagyobbEgyenleguSzamla(Tulajdonos tulajdonos){
        Szamla maxSzamla = new Szamla(tulajdonos);

        if (szamlaLista.contains(tulajdonos)){
            for (int i = 0; i < szamlaLista.size(); i++) {
                if (maxSzamla.getAktualisEgyenleg() < szamlaLista.get(i).getAktualisEgyenleg()){
                    maxSzamla.equals(szamlaLista.get(i));
                }
            }
        }
        return maxSzamla;
    }

    public long getOsszhitelkeret(){
        List<HitelSzamla> hitelSzamlaLista = new ArrayList<>();

        long osszHitelKeret = 0;

        for (int i = 0; i < szamlaLista.size(); i++) {
            osszHitelKeret += szamlaLista.get(i).
        }

        return 0;
    }
}