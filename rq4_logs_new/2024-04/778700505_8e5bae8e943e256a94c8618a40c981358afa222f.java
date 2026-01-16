/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package model;

import java.time.LocalDate;
import java.util.ArrayList;

/**
 *
 * @author zakina
 */
public class Intervention {
    
    private int id;
    private String lieu ;
    private LocalDate heureAppel;
    private LocalDate heureArrivee;
    private int duree;
    private ArrayList<Pompier> lesPompiers ;

    public Intervention() {
    }

    public Intervention(int id){
        this.id = id;
    }
    
    public Intervention (int id, String lieu, LocalDate heureAppel, LocalDate heureArrivee, int Duree, ArrayList<Pompier> lesPompiers){
        this.id = id;
        this.lieu = lieu;
        this.heureAppel = heureAppel;
        this.heureArrivee = heureArrivee;
        this.duree = duree;
        this.lesPompiers = lesPompiers;
    }
    
    public int getId() {
        return id;
    }

    public String getLieu() {
        return lieu;
    }

    public LocalDate getHeureAppel() {
        return heureAppel;
    }

    public LocalDate getHeureArrivee() {
        return heureArrivee;
    }

    public int getDuree() {
        return duree;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setLieu(String lieu) {
        this.lieu = lieu;
    }

    public void setHeureAppel(LocalDate heureAppel) {
        this.heureAppel = heureAppel;
    }

    public void setHeureArrivee(LocalDate heureArrivee) {
        this.heureArrivee = heureArrivee;
    }

    public void setDuree(int duree) {
        this.duree = duree;
    }

    public ArrayList<Pompier> getLesPompiers() {
        return lesPompiers;
    }

    public void setLesPompiers(ArrayList<Pompier> lesPompiers) {
        this.lesPompiers = lesPompiers;
    }
    
    public void addPompier(Pompier p){
        if (lesPompiers == null){
            lesPompiers = new ArrayList<Pompier>();
        }
        lesPompiers.add(p);
    }
    
}