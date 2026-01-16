/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Ventanas;

import Jugador.Jugador;
import java.awt.Color;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingConstants;

/**
 *
 * @author LEONARDO
 */
public class VentanaDeMenu extends JFrame {
   
    private JButton btnJugar;
    private JButton btnInstruciones;
    private JPanel jpContenidoMenu;
    private Background jpBackground;
    public VentanaDeMenu(){
        iniciarComponentes();
                //Configuración de la ventana
        setTitle("Figuras, formas >> Tamaños");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(790,483);
        setLocationRelativeTo(null);
        setVisible(true); 
        setResizable(false);
        setLayout(null);
    }
    
    private void iniciarComponentes(){

        
        Toolkit miPantalla = Toolkit.getDefaultToolkit();
        
        jpBackground = new Background("/imagenes/fondo.png"); 
        jpContenidoMenu = new JPanel();
        
        jpContenidoMenu.setSize(790,483);        
        jpContenidoMenu.setBounds(0,0, 790, 483);
        //jpContenidoMenu.setBackground(new Color(191,177,210));
        jpContenidoMenu.setLayout(null);
        
        
        jpBackground.setSize(790,483);
        
        add(jpBackground);
        add(jpContenidoMenu);
        
        btnJugar = new JButton ("Jugar");
        btnJugar.setBounds(320, 180, 150, 35);    
        jpContenidoMenu.add(btnJugar);
        
        btnInstruciones = new JButton ("Instrucciones");
        btnInstruciones.setBounds(320,240, 150,35);
        
        jpContenidoMenu.add(btnInstruciones);
       
                     
                

    }
}