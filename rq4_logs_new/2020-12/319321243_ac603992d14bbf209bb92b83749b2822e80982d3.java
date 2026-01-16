////////////////////////////////////////////////////////////////////
// [STEFANO] [DAL POZ] [1204683]
////////////////////////////////////////////////////////////////////
package it.unipd.tos.model;

import java.time.LocalTime;
import java.util.List;

import it.unipd.tos.business.User;

public class Ordine {
    List<MenuItem> ordine; 
    User user;
    LocalTime orarioOrdine;
    double prezzo;

    public Ordine(List<MenuItem> ordine, User user, LocalTime orarioOrdine, 
            double prezzo) {
        if(ordine.isEmpty()) {
            throw new IllegalArgumentException("Ordine vuoto");
        }
        if(user == null) {
            throw new IllegalArgumentException("Utente non valido");
        }
        if(orarioOrdine == null) {
            throw new IllegalArgumentException("Orario non valido");
        }
        this.ordine = ordine;
        this.user = user;
        this.orarioOrdine = orarioOrdine;
        this.prezzo = prezzo;
    }

    public List<MenuItem> getOrdine() {
        return ordine;
    }

    public User getUser() {
        return user;
    }

    public LocalTime getOrarioOrdine() {
        return orarioOrdine;
    }

    public double getPrezzo() {
        return prezzo;
    }

    public void setPrezzo(double prezzo) {
        this.prezzo = prezzo;
    }
} 