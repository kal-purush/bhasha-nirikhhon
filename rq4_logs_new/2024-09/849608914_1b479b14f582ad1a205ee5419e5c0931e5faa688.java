package pe.edu.upc.waba.dtos;

import jakarta.persistence.Column;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

public class RecursoDTO {
    private  int idRecu;
    private String cicloRecu;

    private  Curso cu;
    private  User us;

    public int getIdRecu() {
        return idRecu;
    }

    public void setIdRecu(int idRecu) {
        this.idRecu = idRecu;
    }

    public String getCicloRecu() {
        return cicloRecu;
    }

    public void setCicloRecu(String cicloRecu) {
        this.cicloRecu = cicloRecu;
    }

    public Curso getCu() {
        return cu;
    }

    public void setCu(Curso cu) {
        this.cu = cu;
    }

    public User getUs() {
        return us;
    }

    public void setUs(User us) {
        this.us = us;
    }
}