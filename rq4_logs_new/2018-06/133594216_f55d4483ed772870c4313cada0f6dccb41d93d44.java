package Controller;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author luiz_
 */

public class Acoes {

    Model.DAO dao;

    public Acoes(Model.DAO dao){
        this.dao = dao;
    }

    public void alimentar(){
        System.out.println("Botao foi clicado");
    }

}