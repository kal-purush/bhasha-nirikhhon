package models;

import java.util.Date;

public class Avaliacao {
    private int idUsuario;
    private int idLivro;
    private Date data;
    private int nota;

    public Avaliacao(int idUsuario, int idLivro, int nota) {
        this.idUsuario = idUsuario;
        this.idLivro = idLivro;
        this.nota = nota;
    }

    public Avaliacao(int int1, int int2, java.sql.Date date, int int3) {
    }

    public int getIdUsuario() {
        return idUsuario;
    }

    public void setIdUsuario(int idUsuario) {
        this.idUsuario = idUsuario;
    }

    public int getIdLivro() {
        return idLivro;
    }

    public void setIdLivro(int idLivro) {
        this.idLivro = idLivro;
    }

    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public int getNota() {
        return nota;
    }

    public void setNota(int nota) {
        this.nota = nota;
    }

    @Override
    public String toString() {
        return "Avaliacao [idUsuario=" + idUsuario + ", idLivro=" + idLivro + ", data=" + data + ", nota=" + nota + "]";
    }
   
}