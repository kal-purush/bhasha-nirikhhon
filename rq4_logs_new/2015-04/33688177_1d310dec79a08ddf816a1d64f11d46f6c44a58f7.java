/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package taller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;

/**
 *
 * @author alvaroms
 */
public class Lectura {

    double cx1;
    double cy1;
    double r1;
    double cx2;
    double cy2;
    double r2;
    Vector<Vector<Double>> puntos;

    public double getCx1() {
        return cx1;
    }

    public void setCx1(double cx1) {
        this.cx1 = cx1;
    }

    public double getCy1() {
        return cy1;
    }

    public void setCy1(double cy1) {
        this.cy1 = cy1;
    }

    public double getR1() {
        return r1;
    }

    public void setR1(double r1) {
        this.r1 = r1;
    }

    public double getCx2() {
        return cx2;
    }

    public void setCx2(double cx2) {
        this.cx2 = cx2;
    }

    public double getCy2() {
        return cy2;
    }

    public void setCy2(double cy2) {
        this.cy2 = cy2;
    }

    public double getR2() {
        return r2;
    }

    public void setR2(double r2) {
        this.r2 = r2;
    }

    public Vector<Vector<Double>> getPuntos() {
        return puntos;
    }

    public void setPuntos(Vector<Vector<Double>> puntos) {
        this.puntos = puntos;
    }

    public void lecturaArchivo(File archivoLeer) {

        FileReader fr = null;
        BufferedReader br = null;

        try {
            // Apertura del fichero y creacion de BufferedReader para poder
            // hacer una lectura comoda (disponer del metodo readLine()).

            fr = new FileReader(archivoLeer);
            br = new BufferedReader(fr);

            // Lectura del fichero
            String linea;
            linea = br.readLine();
            
            String[] token = linea.split("(,)");
            
            while ((linea = br.readLine()) != null) {
                System.out.println(linea);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // En el finally cerramos el fichero, para asegurarnos
            // que se cierra tanto si todo va bien como si salta 
            // una excepcion.
            try {
                if (null != fr) {
                    fr.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

    }



    
}