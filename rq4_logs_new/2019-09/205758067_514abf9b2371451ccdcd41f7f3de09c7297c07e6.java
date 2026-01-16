/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import InternalFrame.iFcliente;
import Modelo.ClienteDAO;
import Modelo.Cliente;
import Servicios.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Timestamp;
import java.util.ArrayList;
import javax.swing.JOptionPane;

/**
 *
 * @author Usuario
 */
public class ControllerCliente {
    //Creación de vista y model Cliente  
    iFcliente vista;
    ClienteDAO modelo;
    public ControllerCliente(iFcliente vista, ClienteDAO modelo) {

        this.vista = vista;
        this.modelo = modelo;
        
        this.vista.cargarClientesTabla(modelo.listadoCliente());

        this.vista.getjBnuevo().addActionListener(new ListenerCliente());

    }
    
    
    
    public class ListenerCliente implements ActionListener {

        
            @Override
        public void actionPerformed(ActionEvent ae) {

            if (ae.getSource() == vista.getjBnuevo()) {
                JOptionPane.showMessageDialog(null, "Prueba");
                registrar();
            } else if (ae.getSource() == vista.getjBmodificar()) {
                //actualizar();
            }
        }
        
        
        
        public void registrar() {

        if (vista.getjTClienteID().equals("")) {
            vista.gestionMensajes("Ingrese el código",
                    "Error de Entrada", JOptionPane.ERROR_MESSAGE);
            /*else if (vista.getNivel().trim().
                           equals("Seleccionar ...")){
                   vista.gestionMensajes("Seleccione un nivel",
                           "Error de Entrada", JOptionPane.ERROR_MESSAGE );  */
        } else {
            Cliente cliente = new Cliente();
            cliente.setClienteID(601);//revisar el ingreso null
            //System.out.println("id Cliente "+vista.getjTClienteID().getText());
            
//            cliente.setTiendaIDCliente(1);
//            //System.out.println("id Tienda" +vista.getjTiendaID().getText());
//            
//            cliente.setNombreCliente(vista.getjTnombreCliente().getText());
//            //System.out.println("nombre cliente "+vista.getjTnombreCliente().getText());
//            
//            cliente.setApellidoCliente(vista.getjTApellidos().getText());
//            //System.out.println("apellido "+vista.getjTApellidos().getText());
//            
//            cliente.setCorreoCliente(vista.getjTCorreoCliente().getText());
//            //System.out.println("correo "+vista.getjTCorreoCliente().getText());
//            
//            cliente.setDireccionCliente(Integer.parseInt(vista.getjTDireccion().getText()));
//            //System.out.println("dir "+vista.getjTDireccion().getText());
//            
//            cliente.setCuenta_activo(true);
//            cliente.setFechaCreacion(Fecha.crearFechaDate());
//            
//            cliente.setUltimaActualizacionCliente(Fecha.crearFechaTimeStamp());
//            //System.out.println("fecha "+Fecha.crearFechaTimeStamp());
//            
//            cliente.setActivo(1);
//            //System.out.println("Activo "+vista.getjTActivoInt().getText());
//            
///dd

            
           
            
            //if (tamaño == 0) {
                int resultado = 0;
                //int resultado2 = 0;
                resultado = modelo.grabarCliente(cliente);
                
                if (resultado == 1) {
                    vista.gestionMensajes("Registro Grabado con éxito",
                            "Confirmación", JOptionPane.INFORMATION_MESSAGE);

                    ArrayList <Cliente> listaClientes;
                    listaClientes = modelo.listadoCliente();
                    vista.cargarClientesTabla(listaClientes);

                    //vista.activarControles(false); 
                    //vista.nuevoAction();
                } else {
                    vista.gestionMensajes("Error al grabar",
                            "Confirmación", JOptionPane.ERROR_MESSAGE);
                }
            //} else {
                vista.gestionMensajes("Codigo ya está registrado",
                        "Confirmación",
                        JOptionPane.ERROR_MESSAGE);
            }
        
    }
    
        
        
    }    
    
    
}