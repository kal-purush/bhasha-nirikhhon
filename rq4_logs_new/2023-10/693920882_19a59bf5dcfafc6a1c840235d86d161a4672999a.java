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
public class VentanaDeInstruciones extends JFrame {
   
    private JButton btnJugar;
    private JButton btnVolver;
    private JPanel jpContenidoIns;
    private JLabel jlTituloInstruciones;
    private JLabel jlInstruciones;
    
    
    
   // private Background jpBackground;
    public VentanaDeInstruciones(){
       iniciarComponentes();
    }
    
    private void iniciarComponentes(){

        //Configuración de la ventana
        setTitle("Figuras, formas >> Tamaños");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(790,483);
        setLocationRelativeTo(null);
        setVisible(true); 
        setResizable(false);
        setLayout(null);
        Toolkit miPantalla = Toolkit.getDefaultToolkit();
        jpContenidoIns = new JPanel();
         
       
        jpContenidoIns.setSize(790,483);        
        jpContenidoIns.setBounds(0,0, 790, 483);
        jpContenidoIns.setBackground(new Color(191,177,210));
        jpContenidoIns.setLayout(null);
       
        jlTituloInstruciones = new JLabel("Instrucciones",SwingConstants.CENTER);
        jlInstruciones = new JLabel("<html> <p align = center> El objetivo del juego es elegir de los tres tamaños el que se corresponde con el que está a la izquierda de la línea. <br> <br> Mira la figura que está a la izquierda y luego escoge la figura que creas tiene el mismo tamaño entre las tres que te damos en la parte derecha</p><htmal>",SwingConstants.CENTER);
        

        add(jpContenidoIns);
             
        jlTituloInstruciones.setBounds( 0,60, 790,20);
        jlTituloInstruciones.setForeground(Color.WHITE);
        jlTituloInstruciones.setFont(new Font("arial", Font.BOLD, 25));  
        
        jlInstruciones.setBounds(45,120, 700,150);
        jlInstruciones.setForeground(Color.WHITE);
        jlInstruciones.setFont(new Font("arial", Font.BOLD, 19));
        
        btnJugar = new JButton ("Jugar");
        btnJugar.setBounds(320, 300, 150, 35);    
        jpContenidoIns.add(btnJugar);
        
        btnVolver = new JButton ("volver");
        btnVolver.setBounds(320,350, 150,35);
        
        jpContenidoIns.add(jlInstruciones);
        jpContenidoIns.add(jlTituloInstruciones);
        jpContenidoIns.add(btnVolver);
        
       
        ManejadoraDeEventos manejadoraEventos = new ManejadoraDeEventos();
        
        btnJugar.addActionListener(manejadoraEventos); 
        btnVolver.addActionListener(manejadoraEventos);              
                

    }
    
    
    

    class ManejadoraDeEventos implements ActionListener {

        public ManejadoraDeEventos() {
        }

        @Override
        public void actionPerformed(ActionEvent e) {
            if(e.getSource() == btnJugar){                
                JOptionPane.showMessageDialog(null,"Por favor que acabe ya este Proyecto", 
                 "me la esta pelando", JOptionPane.ERROR_MESSAGE);
            }else if (e.getSource() == btnVolver){
                 dispose(); 
                 VentanaDeMenu ventanaMenu = new VentanaDeMenu();
                 ventanaMenu.setVisible(true); 
            
            }
                
        }
    }
}