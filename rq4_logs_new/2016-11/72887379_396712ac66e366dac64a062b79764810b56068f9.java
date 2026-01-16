/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package caixeiroviajanteguloso;

import java.awt.Color;
import java.awt.Graphics;
import javax.swing.JPanel;

/**
 *
 * @author ogarcia
 */
public class Calculos extends JPanel{
    private double[][] custos;
    private int[] rota;
    private int[][] coordenadas;

    public Calculos(double[][] custos, int[] rota, int[][] coordenadas) {
        this.custos = custos;
        this.rota = rota;
        this.coordenadas = coordenadas;
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

    public int[][] getCoordenadas() {
        return coordenadas;
    }

    public void setCoordenadas(int[][] coordenadas) {
        this.coordenadas = coordenadas;
    }
    
    
    public void paintComponent(Graphics g){
        super.paintComponent(g);
        
        //Desenhando os vértices gerados
        for (int i=0; i<this.custos.length;i++){
           g.fillOval(this.coordenadas[i][0], this.coordenadas[i][1], 10, 10);
           g.drawString(" "+(i), this.coordenadas[i][0], this.coordenadas[i][1]);
        }
        
        //Desenhando uma linha interligando todos os vértices
        int i;
        for (i = 0; i < this.rota.length-1; i++) {
            if(i==0){
                g.setColor(Color.red);
            }else{
                g.setColor(Color.black);
            }
            g.drawLine(this.coordenadas[this.rota[i]][0], 
                    this.coordenadas[this.rota[i]][1],
                    this.coordenadas[this.rota[i+1]][0],
                    this.coordenadas[this.rota[i+1]][1]);
        }
        g.setColor(Color.blue);
        g.drawLine(this.coordenadas[this.rota[i]][0],
                this.coordenadas[this.rota[i]][1],
                this.coordenadas[this.rota[0]][0], 
                this.coordenadas[this.rota[0]][1]);
        
    } 
}