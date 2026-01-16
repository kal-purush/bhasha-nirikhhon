package models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table
public class FilmCategorie
{
	
	@Id
	@SequenceGenerator(name="FilmCategorieId", sequenceName="FilmCategorieIdSequence", allocationSize=1)
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator ="FilmCategorieId")
	
	@Column
	private int id;
	@Column
	private String libelleFilmCategorie;
	
	//********* Constructor
	public FilmCategorie() {}
	
	public FilmCategorie(int id, String libelleFilmCategoriee) {
		super();
		this.id = id;
		this.libelleFilmCategorie = libelleFilmCategoriee;
	}
		
	//********* Getters and Setters
	public int getId() {
		return id;
	}
	public void setId(int idd) {
		this.id = idd;
	}
	public String getLibelleFilmCategorie() {
		return this.libelleFilmCategorie;
	}
	public void setLibelleFilmCategorie(String libelleFilmCategoriee) {
		this.libelleFilmCategorie = libelleFilmCategoriee;
	}
}