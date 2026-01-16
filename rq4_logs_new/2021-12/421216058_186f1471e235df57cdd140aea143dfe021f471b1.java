package com.example.dpa_android.data.model;

import java.io.Serializable;

public class Cliente implements Serializable  {
    public int id;
    public String nome;
    public String cpf;
    public String email;
    public String dataNascimento;
    public String sexo;
    public String arquivo;

    public Cliente(String nome, String cpf, String email, String dataNascimento, String sexo, String arquivo) {
        this.nome = nome;
        this.cpf = cpf;
        this.email = email;
        this.dataNascimento = dataNascimento;
        this.sexo = sexo;
        this.arquivo = arquivo;
    }

    public Cliente(int id, String nome, String cpf, String email, String dataNascimento, String sexo, String arquivo) {
        this.id = id;
        this.nome = nome;
        this.cpf = cpf;
        this.email = email;
        this.dataNascimento = dataNascimento;
        this.sexo = sexo;
        this.arquivo = arquivo;
    }
}