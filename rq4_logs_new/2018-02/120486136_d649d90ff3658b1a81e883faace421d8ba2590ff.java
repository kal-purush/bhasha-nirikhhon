/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Entite;

/**
 *
 * @author asus
 */
public class Categorie_centre {
    private int id_categorie ;
    private String nom_categ ;

    public int getId_categorie() {
        return id_categorie;
    }

    public void setId_categorie(int id_categorie) {
        this.id_categorie = id_categorie;
    }

    public String getNom_categ() {
        return nom_categ;
    }

    public void setNom_categ(String nom_categ) {
        this.nom_categ = nom_categ;
    }

    public Categorie_centre(int id_categorie, String nom_categ) {
        this.id_categorie = id_categorie;
        this.nom_categ = nom_categ;
    }

    @Override
    public String toString() {
        return "categorie_centre{" + "id_categorie=" + id_categorie + ", nom_categ=" + nom_categ + '}';
    }
    
}