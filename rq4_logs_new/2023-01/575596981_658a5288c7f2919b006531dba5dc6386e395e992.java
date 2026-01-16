package ProgramasPruebas;
import java.util.Scanner;

import static ProgramasPruebas.Metodos.isCorrectDC;
import static ProgramasPruebas.Metodos.normalize;

public class CodigoDeBarras {
    // Daniel Alonso Lázaro - 2022

    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        String code, newCompleteCode;

        do{
            System.out.print("Introduce el código de barras: ");
            code = sc.nextLine();
            newCompleteCode = normalize(code);
            if(!code.equalsIgnoreCase("0")){
                if(code.length() <= 8 ){
                    System.out.println(newCompleteCode);
                    System.out.println("Es EAN-8");
                    if(isCorrectDC(newCompleteCode)) System.out.println("SI");
                    else System.out.println("NO");
                }else if(code.length() <= 13) {
                    System.out.println(newCompleteCode);
                    System.out.println("Es EAN-13");
                    if (isCorrectDC(newCompleteCode)) System.out.println("SI");
                    else System.out.println("NO");
                }else{
                    System.out.println("El código de barras no es válido.");
                }
            }
        }while(!code.equalsIgnoreCase("0"));
    }
}