package com.tony.example.hsports.mycareapp;

import android.media.Image;
import android.text.AutoText;
import java.util.ArrayList;

/**
 * Created by JamesWalker on 09/02/16.
 */
public class Animal {
    // Fields
    private String name;        // ex. "Tim"
    private String species;    // ex. Puma concolor coryi
    private String birthdate;
    // private String deathDate;     // for keeping memorials of deceased animals
    private AutoText biography;
    private ArrayList<Image> photographs;

    public Animal(String name, String species, String birthdate, AutoText biography){
        // TODO Finish constructor using each field above as arguments
        this.name = name;
        this.species = species;
        this.birthdate = birthdate;
        this.biography = biography;
    }

    public void displayAnimal(){
        // Visit http://www.thecarefoundation.org/meet-our-animals/big-cats/ to see how it's displayed

    }


    // Getters and Setters
    public String getBirthdate() {
        // TODO Format birthdate as Month Day, Year
        return birthdate.toString();
    }

//    public String getDeathDate() {
//        // TODO Format death date as Month Day, Year
//        return deathDate.toString();
//    }

    public void setBirthdate(String birthdate) {
        this.birthdate = birthdate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public AutoText getBiography() {
        return biography;
    }

    public void setBiography(AutoText biography) {
        this.biography = biography;
    }

//    public void setDeathDate(String deathDate) {
//        this.deathDate = deathDate;
//    }

    public ArrayList<Image> getPhotographs() {
        return photographs;
    }

    public void addPhotograph(Image thisImage) {
        photographs.add(thisImage);
    }
}