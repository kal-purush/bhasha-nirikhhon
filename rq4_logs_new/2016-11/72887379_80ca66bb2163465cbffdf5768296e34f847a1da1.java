/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package caixeiroviajanteguloso;

import java.util.Random;

/**
 *
 * @author Otho
 */
public class API {
    private int ponto_partida;
    private double[][]custos; 
    private int[] rota;
    
     public int[] getRota() {
      return rota;
    }

    public void setRota(int[] rota) {
        this.rota = rota;
    }

    public API(int ponto_partida, double[][] custos) {
        this.ponto_partida = ponto_partida;
        this.custos = custos;
        this.rota = new int[custos.length];
        
        gerarAleatorio();
        API_calculo();
    }
    
    public API(int ponto_partida, double[][] custos, int[] rota) {
        this.ponto_partida = ponto_partida;
        this.custos = custos;
        this.rota = rota;
        
        API_calculo();
    }
    
     private void gerarAleatorio(){
      //esvaziando o vetor
      for (int i=0;i<this.custos.length; i++){
        this.rota[i]=-1;
      }
      
      this.rota[0]=this.ponto_partida;
      boolean usado;
      int i = 1, aux;
      Random r = new Random();
      
      while (i<this.rota.length) {
        aux=r.nextInt(this.rota.length);
        
        //verificar se esse ponto gerado já não está sendo usado
        usado = false;
        for(int k=0; k<this.rota.length; k++){
            
          if(this.rota[k]==aux){
            usado = true;  
          }
        }
        
        if (!usado){
          this.rota[i]=aux;
          i++;
        }
      }
  }
  
  private void API_calculo(){
    int []melhorRota = new int[this.rota.length];
    int []rotaAtual = new int[this.rota.length];
    double custoRotaAtual, custoMelhorRota;
    //Inicialmente a melhor rota e a rota atual são iguais 
    //ao vetor rota;
    for (int i=0; i<this.rota.length;i++){
      melhorRota[i]=this.rota[i];  
      rotaAtual[i]=this.rota[i];
    }
    
    Calculos calc = new Calculos(this.custos, rotaAtual);
    custoRotaAtual = calc.custoRota();
    custoMelhorRota = custoRotaAtual;
    
    //aplicando a busca local
    int troca;
    boolean melhor = false;    
    for (int i=1; i<this.rota.length-1; i++){

        troca= rotaAtual[i+1];  
        rotaAtual[i+1]=rotaAtual[i];
        rotaAtual[i]=troca;

        //Calculando o custo dessa rota
        calc.setRota(rotaAtual);
        custoRotaAtual= calc.custoRota();

        //este custo foi menor?
        if(custoRotaAtual < custoMelhorRota){
          //Melhorou, atualizando a melhore rota
          melhor = true;
          for (int k = 0; k<rotaAtual.length; k++){
            melhorRota[k]=rotaAtual[k];
          }
          custoMelhorRota=custoRotaAtual;
        }

        //Voltando com a rota original 
        for (int k = 0; k<rotaAtual.length;k++){
          rotaAtual[k]=this.rota[k];
        }

      //atualizando a rota original
      for (int k = 0; k<rotaAtual.length;k++){
          rota[k]=melhorRota[k];
      }
    }
    if (melhor == true){
        API_calculo();
    }
 }
}