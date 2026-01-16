package model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement 
@Entity  
@NamedQueries({
  @NamedQuery(name="guest.all", query="SELECT g FROM Guest g "),
  @NamedQuery(name="guest.id",query="FROM Guest g WHERE g.id=:goscId")
})

public class Guest { 
	@Id 
	@GeneratedValue( strategy = GenerationType.IDENTITY)
	private int id;
	private String name;
    private String surname;
    private String email;
    private String phone;
    private String adres;
    private String card;

    public void setName(String name) {
        this.name = name;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public void setAdres(String adres) {
        this.adres = adres;
    }

    public void setCard(String card) {
        this.card = card;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getEmail() {
        return email;
    }

    public String getPhone() {
        return phone;
    }

    public String getAdres() {
        return adres;
    }

    public String getCard() {
        return card;
    } 
    
    public long getId() {
  		return id;
  	}

  	public void setId(int id) {
  		this.id = id;
  	}
}