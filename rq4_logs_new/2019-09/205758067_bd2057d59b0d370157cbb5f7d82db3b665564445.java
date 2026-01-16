/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import InternalFrame.iFalquiler;
import Modelo.AlquilerDAO;
import Modelo.Pelicula;
import Modelo.PeliculaDAO;
import java.util.ArrayList;
import java.awt.event.*;

/**
 *
 * @author vanes
 */
public class ControllerAlquiler {
    
    private iFalquiler vista;
    private AlquilerDAO modelo;
    private ArrayList <Pelicula> listadoPelicula;

    public ControllerAlquiler(iFalquiler vista, AlquilerDAO modelo) {
        
        this.vista = vista;
        this.modelo = modelo;
        
        PeliculaDAO modelPelicula = new PeliculaDAO();
        
         //Se carga en el JList la informacion proveniente de la base de datos
        
        //this.vista.cargarPeliculasLista(listadoPelicula = modelPelicula.listadoPeliculas());
        
       ListenerAlquiler escucha = new ListenerAlquiler();
       vista.getjTBuscador().addKeyListener(escucha);
        
        
    }
    
    public class ListenerAlquiler implements ActionListener, KeyListener{
        

        @Override
        public void actionPerformed(ActionEvent ae) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void keyTyped(KeyEvent ke) {
            PeliculaDAO modelPelicula = new PeliculaDAO();
            String buscar = vista.getjTBuscador().getText().trim();
            vista.cargarPeliculasLista(modelPelicula.buscarPeliculas(buscar));
            
        }

        @Override
        public void keyPressed(KeyEvent ke) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void keyReleased(KeyEvent ke) {
            
           // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }



}
    
    
    
    
    
}