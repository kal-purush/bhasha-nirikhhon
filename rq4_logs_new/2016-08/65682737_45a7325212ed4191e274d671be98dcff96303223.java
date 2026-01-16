/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fund_progra;

import java.util.Scanner;

/**
 *
 * @author krlo9
 */
public class UsoString {

    public static void main(String[] args) {
        Scanner intro = new Scanner(System.in);
        int resp=0;
        do{
        String cadena = intro.nextLine();
        imprimirResultados(intro, cadena);
        System.out.println("ingresar nueva cadena? \ningresar 1 para si cualquier otra numero para no");
        resp=intro.nextInt();
        }while(resp==1);

    }

    public static void imprimirResultados(Scanner intro, String cadena) {
        System.out.println(leerString(cadena));
        System.out.println("La cadena tiene " + largoCadena(cadena) + " caracteres");
        System.out.println("La cadena tiene " + contarVocales(cadena) + " vocales");
        System.out.println("La cadena tiene " + contarConsonantes(cadena, contarVocales(cadena)) + " consonantes");
        System.out.println(invertirString(cadena));
        if (existeChar(cadena, leerchar(intro)) == true) {
            System.out.println("El caracter existe en la cadena");
        } else {
            System.out.println("El caracter no se encuentra en la cadena");
        }
        if (existeSubString(cadena, subcadena(intro)) == true) {
            System.out.println("La subcadena si existe dentro de la cadena");
        } else {
            System.out.println("La subcadena no existe dentro de la cadena");
        }
        System.out.println("Array char creado");
        crearArrayChar(cadena);
    }

    public static String leerString(String str) {
        System.out.println("La cadena es "+str);
        return str;
    }

    public static int largoCadena(String str) {
        int length = str.length();
        return length;
    }

    public static int contarVocales(String str) {
        int cont = 0;
        str = str.toLowerCase();
        for (int j = 0; j < str.length(); j++) {
            if ((str.charAt(j) == 'a') || (str.charAt(j) == 'e') || (str.charAt(j) == 'i')
                    || (str.charAt(j) == 'o') || (str.charAt(j) == 'u')) {
                cont++;
            }
        }
        return cont;
    }

    public static int contarConsonantes(String str, int voc) {
        int cont = 0, cons = 0;
        str = str.toLowerCase();
        for (int j = 0; j < str.length(); j++) {
            if ((str.charAt(j) == ' ') || (str.charAt(j) == '-') || (str.charAt(j) == '.') || (str.charAt(j) == ',') || (str.charAt(j) == '0')
                    || (str.charAt(j) == '1') || (str.charAt(j) == '2') || (str.charAt(j) == '3') || (str.charAt(j) == '4') || (str.charAt(j) == '5')
                    || (str.charAt(j) == '6') || (str.charAt(j) == '7') || (str.charAt(j) == '8') || (str.charAt(j) == '9')) {
                cont++;
            }
        }
        cons = str.length() - cont - voc;
        return cons;
    }

    public static String invertirString(String str) {
        String nstr = "";
        for (int j = str.length() - 1; j >= 0; j--) {
            nstr = nstr + str.charAt(j);
        }
        return nstr;
    }

    public static char leerchar(Scanner intro) {
        System.out.println("Ingresar caracter");
        char x = intro.next("[a-zA-Z]").charAt(0);
        return x;
    }

    public static boolean existeChar(String str, char x) {
        int cont = 0;
        for (int j = 0; j < str.length(); j++) {
            if (str.charAt(j) == x) {
                cont++;
            }
        }
        if (cont > 0) {
            return true;
        } else {
            return false;
        }
    }

    public static String subcadena(Scanner intro) {
        System.out.println("Ingresar cadena");
        String substr = intro.nextLine();
        return substr;
    }

    public static boolean existeSubString(String str, String substr) {
        int existe = str.indexOf(substr);
        if (existe != 1) {
            return true;
        } else {
            return false;
        }
    }

    public static char[] crearArrayChar(String str) {
        char array[] = new char[str.length()];
        for (int j = 0; j < str.length(); j++) {
            array[j] = str.charAt(j);
        }
        return array;
    }

}