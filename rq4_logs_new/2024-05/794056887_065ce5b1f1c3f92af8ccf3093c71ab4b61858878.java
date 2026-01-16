package net.hiperdino.clientes;

import java.util.Stack;

import net.hiperdino.productos.Producto;
import net.hiperdino.utilidades.Generador;

public class Cliente {
  protected String nombre = Generador.nombreAleatorio();
  protected int edad ;
  protected Stack<Producto> cestaCompra = new Stack<>();
  
  public Cliente (int edad){
    this.edad = edad;
  }

  public String getNombre() {
    return nombre;
  }

  public void setNombre(String nombre) {
    this.nombre = nombre;
  }

  public int getEdad() {
    return edad;
  }

  public void setEdad(int edad) {
    this.edad = edad;
  }

  public Stack<Producto> getCestaCompra() {
    return cestaCompra;
  }

  public void setCestaCompra(Stack<Producto> cestaCompra) {
    this.cestaCompra = cestaCompra;
  }
  
  public void llenarCesta(){
    for (int i = 0; i < Generador.tamañoCestaAleatorio(); i++) {
      this.cestaCompra.push(Generador.productoAleatorio());
    }
  } 

  @Override
  public String toString() {
    String descripcion = "================================= \n * Nombre: ";
    descripcion += nombre + "\n * Total de productos: ";
    descripcion += nombre + "\n * Total de productos: ";
    descripcion += cestaCompra.size() + "\n" + "*  Lista de artículos en la cesta: ";
    for (Producto producto : cestaCompra) {
      descripcion += "    " + producto.getNombre();
    }
    return  descripcion;
  }
  
}