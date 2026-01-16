/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ingresarordenesapp;

/**
 *
 * @author prof_uide_e
 */
public class IngresarOrdenesAPP {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
         Orden orden = new Orden();
         Menu menu = new Menu("Producto", orden);
    }
    
}