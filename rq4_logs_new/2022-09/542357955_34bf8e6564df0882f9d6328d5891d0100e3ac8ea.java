package pangrama;

import java.util.*;

public class Pangrama {

    public static void main(String[] args) {
        Scanner leer = new Scanner(System.in);
        int sum = 0;
        int a = leer.nextInt();
        String alfabeto = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < a; i++) {
            String respuesta = leer.nextLine();
            String aux = "";
            for (int j = 0; j < respuesta.length(); j++) {
                String x = "" + respuesta.charAt(j);
                x = x.toLowerCase();
                for (int k = 0; k < alfabeto.length(); k++) {
                    String y = "" + alfabeto.charAt(k);
                    if (y.equalsIgnoreCase(x)) {
                        for (int l = 0; l <= aux.length(); l++) {
                            if (aux.contains(x)) {
                            } else {
                                aux = aux + x;
                                sum = sum + 1;
                            }
                        }
                    } 
                }
            }
            if (aux.length() >= 26) {
                System.out.println("SI");
            } else {
                System.out.println("NO");
            }
        }

    }

    
}