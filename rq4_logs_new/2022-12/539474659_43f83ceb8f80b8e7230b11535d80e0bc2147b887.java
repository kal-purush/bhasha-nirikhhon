/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ReservationDataLayer.entities;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

/**
 *
 * @author Arkios
 */
public class Accessemployes implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int numeroEmploye;
    private String habilitation;

    public Accessemployes(int numeroEmploye, String habilitation) {
        this.numeroEmploye = numeroEmploye;
        this.habilitation = habilitation;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + this.numeroEmploye;
        hash = 67 * hash + Objects.hashCode(this.habilitation);
        return hash;
    }

    @Override
    public String toString() {
        return "Accessemployes{" + "numeroEmploye=" + numeroEmploye + ", habilitation=" + habilitation + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Accessemployes other = (Accessemployes) obj;
        if (this.numeroEmploye != other.numeroEmploye) {
            return false;
        }
        if (!Objects.equals(this.habilitation, other.habilitation)) {
            return false;
        }
        return true;
    }

    public int getNumeroEmploye() {
        return numeroEmploye;
    }

    public void setNumeroEmploye(int numeroEmploye) {
        this.numeroEmploye = numeroEmploye;
    }

    public String getHabilitation() {
        return habilitation;
    }

    public void setHabilitation(String habilitation) {
        this.habilitation = habilitation;
    }

    public Accessemployes() {
    } 
}