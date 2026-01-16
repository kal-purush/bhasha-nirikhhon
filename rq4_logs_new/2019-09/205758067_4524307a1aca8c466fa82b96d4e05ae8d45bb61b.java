/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import JFrame.jFinventario;
import Modelo.InventarioDAO;

/**
 *
 * @author Usuario
 */
public class ControllerInventario {

    public ControllerInventario(jFinventario vista,InventarioDAO modelo) {
        
        
        
        vista.cargarInfoAlquilerTabla(modelo.extraerinfoAlquiler());
        
        //vista.cargarInfoInventarioTabla(modelo.extraerInfoInventarios());
        
        
    }
    
    
    
}