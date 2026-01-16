package br.com.zup.TaDentro.colaborador;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
@Table (name = "colaboradores")
public class Colaborador {

    private String nome;
    private String email;
    @Id
    @GeneratedValue (strategy = GenerationType.IDENTITY)
    private String matricula;

    public Colaborador() {
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getMatricula() {
        return matricula;
    }

    public void setMatricula(String matricula) {
        this.matricula = matricula;
    }
}