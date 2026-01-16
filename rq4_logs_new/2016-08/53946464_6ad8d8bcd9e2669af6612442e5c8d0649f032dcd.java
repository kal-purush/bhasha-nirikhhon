/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CODIGOS;

import javax.swing.JOptionPane;

/**
 *
 * @author irmoura
 */
public class Caixa_Eletronico {
    
    public static int valor_liberado;
    public static int notas_de_10, notas_de_20, notas_de_50, notas_de_100;
    
    public static void main(String args[]){
        
        String valor_solicitado = JOptionPane.showInputDialog(null,"Digite o valor que deseja sacar :");
        
        int valor_soli_int = Integer.parseInt(valor_solicitado);
        
        for(int i= 0; i < valor_soli_int; i++){
            valor_liberado++;
            if(valor_liberado%10 == 0){
                notas_de_10++;
            }
            if(valor_liberado%20 == 0){
                notas_de_20++;
            }
            if(valor_liberado%50 == 0){
                notas_de_50++;
            }
            if(valor_liberado%100 == 0){
                notas_de_100++;
            }
        }
        
        JOptionPane.showMessageDialog(null,"Quantidade de notas de 10 :"+notas_de_10+"\n"
        +"Quantidade de notas de 20 :"+notas_de_20+"\n"
        +"Quantidade de notas de 50 :"+notas_de_50+"\n"
        +"Quantidade de notas de 100 :"+notas_de_100+"\n"
        +"Valor solicitado :"+valor_soli_int);
        
    }
}