import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * RD
 */

public class RD {
    static int posicion = 0;
    static String cabeza, entrada;
    static String lex, renglon;

    public static void lee_token(File xFile) {
        try {
            FileReader fr = new FileReader(xFile);
            BufferedReader br = new BufferedReader(fr);
            long noSirve = br.skip(posicion);
            String linea = br.readLine();
            posicion = posicion + linea.length() + 2;
            cabeza = linea;
            linea = br.readLine();
            posicion = posicion + linea.length() + 2;
            lex = linea;
            linea = br.readLine();
            posicion = posicion + linea.length() + 2;
            renglon = linea;
            fr.close();
            System.out.println(".");
        } catch (IOException e) {
            System.out.println("Errorsote");
        }
    }

    public static void asocia(String token) {
        if (cabeza.equals(token)) {
            lee_token(xArchivo(entrada));
        } else {
            rut_error();
        }
    }

    public static File xArchivo(String xName) {
        File xFile = new File(xName);
        return xFile;
    }

    public static void rut_error() {
        System.out.println("\n\nError sintactico(" + renglon + ")):  compilacion terminada, en el caracter["
                + cabeza + "] !!!!\n");
        System.exit(4);
    }

    public static void E() {
        if (cabeza.equals("id") || cabeza.equals("num")) {
            A();
            S();
        } else {
            rut_error();
        }
    }

    public static void A() {
        if (cabeza.equals("id")) {
            asocia("id");

        } else if (cabeza.equals("num")) {
            asocia("num");
        } else {
            rut_error();
        }
    }

    public static void S() {
        if (cabeza.equals("mas")) {
            asocia("mas");
            E();
        } else if (cabeza.equals("menos")) {
            asocia("menos");
            E();
        } else if (cabeza.equals("*")) {
            asocia("*");
            E();
        } else if (cabeza.equals("/")) {
            asocia("/");
            E();
        }
    }

    public static String pausa() {
        BufferedReader entrada = new BufferedReader(new InputStreamReader(System.in));
        String nada = null;
        try {
            nada = entrada.readLine();
            return (nada);
        } catch (Exception e) {
            System.err.println(e);
        }
        return ("");
    }

    public static void main(String[] args) {
        entrada = args[0] + ".cm1";
        lee_token(xArchivo(entrada));
        E();
        if (cabeza.equals("fin")) {
            System.out.println("Analisis sintactico correcto");
        } else {
            rut_error();
        }
    }

}