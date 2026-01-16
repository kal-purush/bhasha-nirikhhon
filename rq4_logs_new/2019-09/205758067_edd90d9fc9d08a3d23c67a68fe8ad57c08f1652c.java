/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import JFrame.InterfazPrincipal;
import Modelo.EmpleadoDAO;

/**
 *
 * @author vanes
 */
public class ControllerInterfaz {
    
    private InterfazPrincipal vista;
    private EmpleadoDAO modelo;

    public ControllerInterfaz(InterfazPrincipal vista, EmpleadoDAO modelo) {

        this.vista = vista;
        this.modelo = modelo;

    }
    
    
    
}