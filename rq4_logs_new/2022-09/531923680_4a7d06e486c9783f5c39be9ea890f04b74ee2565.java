public class SentenciasBucleEtiquetasBuscar {
    public static void main(String[] args) {

        String frase = "trigo tres tristes tigres tragan trigo  en un trigal";
        String palabra = "trigo";

        int maxPalabra = palabra.length();
        int maxFrase = frase.length() - maxPalabra;


        int cantidad = 0;
        char letra = 'g';
        bucleCaracter:
        for (int i = 0; i < maxFrase;)
        {
            int aux = i;
            for (int j = 0; j <= maxPalabra; j++){
                if(frase.charAt(aux++) != palabra.charAt(j)){
                    i++;
                    continue bucleCaracter;
                }
            }

            cantidad ++;
            i = i + maxPalabra;

        }
        System.out.println("cantidad de veces '" + palabra +"' encontrada en la frase = " + cantidad );
    }
}