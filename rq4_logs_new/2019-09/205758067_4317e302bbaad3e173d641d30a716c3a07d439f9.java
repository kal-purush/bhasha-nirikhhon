/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import JFrame.jFCiudad;
import Modelo.Ciudad;
import Modelo.CiudadDAO;

/**
 *
 * @author vanes
 */
public class ControllerCiudad {
    
    jFCiudad vista;
    CiudadDAO modelo;

    public ControllerCiudad(jFCiudad vista, CiudadDAO modelo) {
        this.vista = vista;
        this.modelo = modelo;
    }
    
  
    
    
}