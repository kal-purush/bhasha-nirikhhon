/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bancobianka;

/**
 *
 * @author aluno
 */
public class cliente extends conta {

    /**
     * @return the idade
     */
    public int getIdade() {
        return idade;
    }

    /**
     * @param idade the idade to set
     */
    public void setIdade(int idade) {
        this.idade = idade;
    }

    /**
     * @return the nome
     */
    public String getNome() {
        return nome;
    }

    /**
     * @param nome the nome to set
     */
    public void setNome(String nome) {
        this.nome = nome;
    }

    /**
     * @return the RG
     */
    public int getRG() {
        return RG;
    }

    /**
     * @param RG the RG to set
     */
    public void setRG(int RG) {
        this.RG = RG;
    }

    /**
     * @return the CPF
     */
    public int getCPF() {
        return CPF;
    }

    /**
     * @param CPF the CPF to set
     */
    public void setCPF(int CPF) {
        this.CPF = CPF;
    }

    /**
     * @return the dt_nasc
     */
    public int getDt_nasc() {
        return dt_nasc;
    }

    /**
     * @param dt_nasc the dt_nasc to set
     */
    public void setDt_nasc(int dt_nasc) {
        this.dt_nasc = dt_nasc;
    }
    private String nome;
    private int RG;
    private int CPF;
    private int dt_nasc;
    private int idade;
}