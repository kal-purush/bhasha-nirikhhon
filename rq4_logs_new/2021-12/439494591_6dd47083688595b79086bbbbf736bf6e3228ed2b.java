package com.ciclo3.recuperacion.model;

import java.io.Serializable;
import javax.persistence.*;

@Entity
@Table(name = "artista")
public class Artista implements Serializable {
    @Id
    @GeneratedValue (strategy = GenerationType.IDENTITY)
    private Integer idArtista;
    private String name;
    private String pais;

    public Integer getIdArtista() {
        return idArtista;
    }
    public void setIdArtista(Integer idArtista) {
        this.idArtista = idArtista;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getPais() {
        return pais;
    }
    public void setPais(String pais) {
        this.pais = pais;
    }
    
    
}