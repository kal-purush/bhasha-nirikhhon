/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package problema4;
import java.util.*;
/**
 *
 * @author Raziel 2
 */
public class Problema4 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        float c,f; //grados celsius y cesius
        c = solicitarDato(); // solocitar °C
        f = convertirGrados(c); // convertir a grados Farenheit
        mostrarFarenheit(c,f); //mostrar °C convertidos a °F
    }
    public static float solicitarDato(){
        float c;
        Scanner teclado = new Scanner(System.in);
        System.out.println("Introduce los grados Celsius");
        c = teclado.nextFloat();
        return c;
    }
    public static float convertirGrados(float c){
        float f;
        f = c * 9/5 + 32; 
        return f;
    }
    public static void mostrarFarenheit(float c,float f) {
        System.out.println(c +"°C equivalen a:" + f+"°F");
    }
}