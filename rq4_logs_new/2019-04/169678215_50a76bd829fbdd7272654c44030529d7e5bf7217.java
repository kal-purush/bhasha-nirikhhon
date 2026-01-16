package Main;

import file.GerenciadorArquivo;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Familia
 */

public class Main {    
    /**Verificar se está faltando parâmetros */
    private static void verificarParametros(String[] args) throws Exception {        
        if (args.length == 0 || args.length < 4) {
            throw new Exception("parâmetros faltando");
        }
    }

    public static void main(String[] args) {            
        try {
            //verificarParametros(args);
            GerenciadorArquivo.getIps();
            GerenciadorArquivo.gravarLog();
            
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }

    }

}