package net.hiperdino.utilidades;

import javax.swing.JOptionPane;

public class Menu {
  public String printMenu(){
    String mensaje = """
        1- Abrir caja
        2- Añadir un nuevo cliente a la cola 
        3- Atender un cliente
        4- Ver clientes pendientes
        5- Cerrar supermercado
        """;
    return JOptionPane.showInputDialog(null, mensaje);
  }
}