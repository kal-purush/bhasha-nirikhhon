package ecommerce;

public class NameUtils {


    public static String capitalizarTexto(String textoSinFormato){
        String []palabras = textoSinFormato.split(" ");
        StringBuilder textoFormateado = new StringBuilder();

        for(String palabra : palabras){
            textoFormateado.append(palabra.substring(0,1).toUpperCase()
                    .concat( palabra.substring(1,palabra.length())
                            .toLowerCase()).concat(" "));
        }

        return textoFormateado.toString().trim();
    }
}