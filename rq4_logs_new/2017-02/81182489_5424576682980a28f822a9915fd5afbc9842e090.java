/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package boletin.pkg18;

import static IntroducirDatos.Datos.*;
import javax.swing.JOptionPane;
/**
 *
 * @author Diego
 */
public class Boletin18 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        int selec;
        Buzon buz = new Buzon();
        do{
            int cont = 1;
            selec = Integer.parseInt(JOptionPane.showInputDialog("1: Introducir correo. \n2: Mostrar correo concreto. \n3: Mostrar número de correos.\n4: Mostrar si quedan correos sin leer. \n5: Primer correo sin leer. \n6: Eliminar un correo."));
            switch(selec){
                case 1: buz.añadeCorreo(new Correo(cont, pedirString("Nombre origen"), pedirBoolean("True: El correo ya fue leído. \nFalse: El correo no fue leído.")));
                        break;
                        
                case 2: buz.mostrar(pedirInt("Numero del correo que quieres mostrar."));
                        break;
                
                case 3: buz.mostrarNCorreos();
                        break;
                        
                case 4: buz.mostrarSinL();
                        break;
                      
                case 5: buz.primeroNoLeido();
                        break;
                        
                case 6: buz.eliminar(pedirInt("Numero del correo que quieres eliminar."));
                        break;
                        
            }
            cont++;
        }while(selec<7);
        
        
    }
    
}