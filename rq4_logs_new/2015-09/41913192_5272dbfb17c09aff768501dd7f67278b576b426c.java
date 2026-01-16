package com.mycompany.uutislukija;

import com.mycompany.hackernewsuutiset.HackerPaivanUutiset;
import static com.mycompany.uutislukija.Main.komennot;

public class Lukija {

    HackerPaivanUutiset hakija;

    public String lue(String[] args) {
        return lue(args, new HackerPaivanUutiset());
    }

    public String lue(String[] args, HackerPaivanUutiset hakija) {
        this.hakija = hakija;
        if (args.length == 0 || args[0].isEmpty()) {
            return "Et antanut komentoa. Käytettävissä olevat komennot: " + komennot();
        } else if (args[0].equals("viimeisin")) {
            return this.hakija.haeViimeisinUutinen();
        } else if (args[0].equals("suosituin")) {
            return this.hakija.haeSuosituinUutinen();
        } 
        return "Et antanut komentoa. Käytettävissä olevat komennot: " + komennot();
    }
}