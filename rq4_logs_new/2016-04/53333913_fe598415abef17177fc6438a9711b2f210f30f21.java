/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projekt;

import javafx.beans.property.SimpleStringProperty;

/**
 *
 * @author Maciek
 */
public class Project {
   private final SimpleStringProperty Nazwa;
   private final SimpleStringProperty Opis;
   private final SimpleStringProperty Poczatek;
   private final SimpleStringProperty Koniec;

	public Project(String Nazwa, String Opis, String Poczatek, String Koniec){

		this.Nazwa = new SimpleStringProperty(Nazwa);
		this.Opis = new SimpleStringProperty(Opis);
                this.Poczatek = new SimpleStringProperty(Poczatek);
		this.Koniec = new SimpleStringProperty(Koniec);
	}
        
public String getNazwa(){
	return Nazwa.get();
}
public String getOpis(){
	return Opis.get();
}

public String getPoczatek(){
	return Poczatek.get();
}
public String getKoniec(){
	return Koniec.get();
}

public void setNazwa(String nazwa){
	Nazwa.set(nazwa);
}
public void setOpis(String opis){
	Opis.set(opis);
}
public void setPoczatek(String poczatek){
	Poczatek.set(poczatek);
}
public void setKoniec(String koniec){
	Koniec.set(koniec);
} 
}