/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import JFrame.Login;
import Modelo.Empleado;
import Modelo.EmpleadoDAO;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import javax.swing.JOptionPane;

/**
 *
 * @author nicol
 */
public class ControllerLogin {
    
    private Login vista;
  //  private EmpleadoDAO modelo;
    
       
  //  public ControllerLogin(Login vista, EmpleadoDAO modelo){
    public ControllerLogin(Login vista){
        this.vista = vista;
       // this.modelo = modelo;
        ListenerLogin escucha = new ListenerLogin();
        this.vista.getIngresar().addActionListener(escucha);
    }             
    //Construccion clase ListenerLogin para manejar los eventos
    public class ListenerLogin implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent ae) {
            
             if (ae.getSource() == vista.getIngresar()){
             entrarTiendaDVD();
            }  
        }      
    }
    
    public void entrarTiendaDVD(){
 
   // ArrayList<Empleado> usuario = modelo.listadoEmpleado();
    JOptionPane.showMessageDialog(null,"Prueba?");
  //   JOptionPane.showInputDialog(null, "Prueba");//usuario.get(0));
    }
}