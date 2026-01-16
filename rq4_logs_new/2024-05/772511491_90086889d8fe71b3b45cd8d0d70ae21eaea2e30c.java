package com.company;

public class Bicolore extends Mattonella{
    Interno interno;
    public Bicolore(String codice, double lato, double prezzo, String materiale, boolean sceltaColore, Interno interno) {
        super(codice, lato, prezzo, materiale, sceltaColore);
        setInterno(interno);
    }

    private void setInterno(Interno interno) {
        this.interno = interno;
    }


}