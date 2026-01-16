package net.hiperdino.cajero;

import java.util.LinkedList;
import java.util.Queue;

import net.hiperdino.clientes.Cliente;

public class Cajero {
  protected int numeroCaja;
  protected boolean abierta;
  protected Queue<Cliente> colaClientes = new LinkedList<>();

  public Cajero(int numeroCaja){
    this.numeroCaja = numeroCaja;
  }

  public int getNumeroCaja() {
    return numeroCaja;
  }

  public void setNumeroCaja(int numeroCaja) {
    this.numeroCaja = numeroCaja;
  }

  public boolean isAbierta() {
    return abierta;
  }

  public void setAbierta(boolean abierta) {
    this.abierta = abierta;
  }

  public Queue<Cliente> getColaClientes() {
    return colaClientes;
  }

  public void setColaClientes(Queue<Cliente> colaClientes) {
    this.colaClientes = colaClientes;
  }

  @Override
  public String toString() {
    String descripcion = "================================= \n * Numero de caja: ";
    descripcion += numeroCaja + "\n * Total de clientes: ";
    descripcion += colaClientes.size() + "\n" + "*  Clientes en fila: ";
    for (Cliente cliente : colaClientes) {
      descripcion += "    " + cliente.getNombre();
    }
    descripcion += "=================================";
    return  descripcion;
  }

  

}