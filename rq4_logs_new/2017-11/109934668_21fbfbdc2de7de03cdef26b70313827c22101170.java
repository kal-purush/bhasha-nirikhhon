package Validacion;

/**
 *
 * @author Parker
 */
public class Validacion {    
    /*
    * Función que verifiva si en una cadena solo hay numeros.
    */
    public static boolean SoloNumeros(String numero){        
        return numero.matches("[0-9]+");
    }
    
    /*
    * Función que verifiva si en una cadena si hay numeros enteros o con punto decimal.
    */
    public static boolean SoloFlotante(String numero){ 
        
        return numero.matches("[0-9]+[\\.][0-9]+") || numero.matches("[0-9]+") ;
    }
    
    /*
    * Función que verifiva si en una cadena solo hay letras.
    */
    public static boolean SoloLetras(String letras){                
        return letras.toLowerCase().matches("[a-z\\á\\é\\í\\ó\\ú\\ñ]+");
    }
    
    /*
    * Función que verifiva si en una cadena solo hay letras.
    */
    public static boolean SoloNombres(String letras){                
        return letras.toLowerCase().matches("[a-z\\á\\é\\í\\ó\\ú\\ñ\\ ]+");
    }
    
    /*
    * Función que verifiva si en una cadena hay numeros y letras.
    */
    public static boolean SoloLetrasYNumeros(String cadena){                
        return cadena.toLowerCase().matches("[a-z0-9]+");
    }
    
    /*
    * Función que verifiva si es un cubículo.
    */
    public static boolean SoloCubiculo(String cadena){                
        return cadena.toLowerCase().matches("[a-z]-[0-9]+");
    }
    
    /*
    * Función que verifiva si en una cadena solo hay numeros, letras y espacios.
    */
    public static boolean SoloLetrasYNumerosYEspacios(String cadena){                
        return cadena.toLowerCase().matches("[a-z0-9\\á\\é\\í\\ó\\ú\\ñ\\ \\.]+");
    }
    
    /*
    * Función que verifiva si en una cadena hay letras, numeros, espacio, punto, parentesis, coma y guion.
    */
    public static boolean SoloTexto(String cadena){
        return cadena.toLowerCase().matches("[a-z\\á\\é\\í\\ó\\ú\\ñ\\ \\.]+");
    }
    
    /*
    * Función que verifiva si en una cadena hay un correo electrónico.
    */
    public static boolean SoloEmail(String cadena){
        String email = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
        return cadena.matches(email);
    }    
}
