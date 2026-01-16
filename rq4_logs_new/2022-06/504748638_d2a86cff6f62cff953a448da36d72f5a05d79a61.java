/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package JBV.App.Clases.Forms;

import jdk.swing.interop.DragSourceContextWrapper;

/**
 *
 * @author Joseph
 */
public class Celular {
    String codigo;
    String marca;
    String modelo;
    double precio;

    public Celular(String codigo, String marca, String modelo, double precio) {
        this.codigo = codigo;
        this.marca = marca;
        this.modelo = modelo;
        this.precio = precio;
    }


    public void general(){
        System.out.println("El codigo del celular es: "+codigo+"\nMarca: "+marca+"\nModelo: "+modelo+"\nPrecio: S/."+precio);
    }
        
       
    public void descuento(){
        double desc;
        desc=precio-(precio*0.1);        
        System.out.println("Felicitaciones ha recibido un descuento del 10% \nPrecio Original: S/."+precio+"\nAhora es: S/."+desc);
    }
}
