package com.example.demo.model;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;

@Entity
public class Vendedor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank(message = "Favor preencher o nome!")
    private String nome;

    @NotBlank(message = "Favor preencher a matricula!")
    private String matricula;

    @NotBlank(message = "Favor informar um CPF válido!")
    private String cpf;

    @NotBlank(message = "Favor informar um e-mail válido!")
    private String email;

    public Vendedor(Long id, String nome, String matricula, String cpf, String email){
        this.id = id;
        this.nome = nome;
        this.matricula = matricula;
        this.cpf = cpf;
        this.email = email;
    }

    public Long getId() { return id; }

    public void setId(Long id) { this.id = id; }

    public String getNome() { return nome; }

    public void setNome(String nome) { this.nome = nome; }

    public String getMatricula() { return matricula; }

    public void setMatricula(String matricula) { this.matricula = matricula; }

    public String getCpf() { return cpf; }

    public void setCpf(String cpf) { this.cpf = cpf; }

    public String getEmail() { return email; }

    public void setEmail(String email) { this.email = email; }
}