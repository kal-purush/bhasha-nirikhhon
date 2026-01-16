package caixeiroviajanteguloso;


import java.util.Random;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author lsiqueira
 */
public class BuscaLocal {
    private int ponto_partida;
    private double[][] custos;
    private int[] rota;
    

    public BuscaLocal(int ponto_partida, double[][] custos) {
        this.ponto_partida = ponto_partida;
        this.custos = custos;
        this.rota = new int[custos.length];
        gerarAleatorio();
        allPairs();
    }

    public BuscaLocal(int ponto_partida, double[][] custos, int[] rota) {
        this.ponto_partida = ponto_partida;
        this.custos = custos;
        this.rota = rota;
        allPairs();
    }

    public int getPonto_partida() {
        return ponto_partida;
    }

    public void setPonto_partida(int ponto_partida) {
        this.ponto_partida = ponto_partida;
    }

    public double[][] getCustos() {
        return custos;
    }

    public void setCustos(double[][] custos) {
        this.custos = custos;
    }

    public int[] getRota() {
        return rota;
    }

    public void setRota(int[] rota) {
        this.rota = rota;
    }
    
    private void gerarAleatorio(){
        //Esvaziando o vetor rota
        
        for(int i = 0; i<this.custos.length; i++){
            this.rota[i] = -1;
        }
        
        this.rota[0] = this.ponto_partida;
        boolean usado = false;
        int i = 1, aux;
        Random r = new Random();
        
        while(i<this.rota.length){
            aux = r.nextInt(this.rota.length);
            //Verifica se este ponto geradonaõ esta sendo usado
            usado = false;
            for(int k = 0; k<this.rota.length;k++){
                if(this.rota[k] ==  aux){
                    usado = true;
                }
            }
            if(!usado){
                this.rota[i]=aux;
                i++;
            }
        }
        
    }
    
    private void allPairs(){
        int[] melhorRota = new int[this.rota.length];
        int[] rotaAtual = new int[this.rota.length];
        double custoRotaAtual, custoMelhorRota;
        
        //Inicialmente a melhor rota e a rota atual são  iguais
        //ao vetor rota
        
        for(int i = 0; i<this.rota.length; i++){
            melhorRota[i]= this.rota[i];
            rotaAtual[i]=this.rota[i];
        }
        
        Calculos calc = new Calculos(this.custos, rotaAtual);
        custoRotaAtual = calc.custoRota();
        custoMelhorRota = custoRotaAtual;
        
        //Aplicando a busca local
        int troca;
        boolean melhorou = false;
        for(int i = 1; i < this.rota.length - 1; i++){
            for(int j=i; j<this.rota.length; j++){
                troca = rotaAtual[i];
                rotaAtual[i] = rotaAtual[j];
                rotaAtual[j] = troca;
                
                //Calculando custo desta rota
                calc.setRota(rotaAtual);
                custoRotaAtual = calc.custoRota();
                
                //Este custo foi menor?
                if(custoRotaAtual < custoMelhorRota){
                    //Melhorou atualizando melhor rota
                    melhorou = true;
                    custoMelhorRota = custoRotaAtual;
                    for(int k = 0; k<rotaAtual.length;k++){
                        melhorRota[k] = rotaAtual[k];
                    }
                }
                //Voltando com a rota original
                for(int k = 0; k<rotaAtual.length;k++){
                   rotaAtual[k] = this.rota[k];
                }
            }
            
            //Atualizando a rota
            for(int k = 0; k<rotaAtual.length;k++){
                this.rota[k] = melhorRota[k];  
            }
            
        }
    }
}