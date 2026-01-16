/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package calcula2a;

/**
 *
 * @author rzarzafernandez
 */


import javax.swing.JOptionPane;


public class Calcula2a {
    
    public static void main(String[] args) {
        Modelo op=new Modelo();
        op.setNum1(Float.parseFloat(JOptionPane.showInputDialog("Introduzca primer número")));
        op.setNum2(Float.parseFloat(JOptionPane.showInputDialog("Introduzca segundo número")));
        int oper=(Integer.parseInt(JOptionPane.showInputDialog("¿Que operacion quieres que haga?\n1.-Suma\n2.-Resta\n3.-Multiplicacion\n4.-Division")));
        
        switch (oper) {
            case 1:
               op.setOperacion(1);
                break;
            case 2:
                op.setOperacion(2);
                break;
            case 3:
                op.setOperacion(3);
                break;
            case 4:
                op.setOperacion(4);
                break;
                
               
                
        }
         float num1=op.getNum1();
         float num2=op.getNum2();
            float resultado=realizaOperacion(op.getNum1(), op.getNum2(), op.getOperacion());
            op.setResultado(resultado);
            
            Vista.imprimir(op);
        }
    
        public static float realizaOperacion(float num1, float num2,int op){
            float res;
            
            
            switch (op) {
                case 1:
                    res=num1+num2;
                    
                    return res;    
                    
                case 2:
                    res=num1-num2;
                   
                    return res;   
                case 3:
                     res=num1*num2;
                    
                    return res;   
                case 4:
                     res=num1/num2;
                     
                    return res;   
                default:
                    return -1;
                        
            }
            
        }

    }