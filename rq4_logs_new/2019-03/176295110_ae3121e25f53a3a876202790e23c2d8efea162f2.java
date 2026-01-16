package com.company;

public class Cliente {
    String nome, ra;
    boolean funcionario, visitante;

    public Cliente(String nome, String ra) {

        setNome(nome);

        setRa(ra);

        funcionario = false;

        visitante = false;

    }

    public Cliente(String nome, boolean isFuncionario) {

        setNome(nome);

        if(isFuncionario == false){

            visitante = false;

        }
        else{

            funcionario = true;

        }

    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public void setRa(String ra) {
        this.ra = ra;
    }

    public void Comprar(){


    }

    public void Acompanhar(){


    }
}